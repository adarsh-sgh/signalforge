"""ClickHouse sink: one `signals_daily` table instead of one OpenSearch index per day.

ReplacingMergeTree keyed on (day, tenant_id, entity_id) turns the pipeline's upserts into plain inserts;
replays leave duplicate rows that merges collapse to the newest `updated_at`, and reads use
FINAL so they see the collapsed view before the merge has run.
"""
import datetime as dt
import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from signalforge.search.store import SearchStore, day_of, doc_key
from signalforge.tenancy import split_doc_id

TABLE = "signals_daily"
COLUMNS = ["day", "tenant_id", "entity_id", "window_start", "window_end", "n", "mean_score", "min_score",
           "max_score", "stddev_score", "last_score", "last_ts", "signal_types", "sources", "updated_at"]
_ISO = "%Y-%m-%dT%H:%M:%SZ"
_UTC = dt.timezone.utc


def _parse_utc(iso: str) -> dt.datetime:
    # tz-aware, else the driver would encode the naive value in the process's local zone
    return dt.datetime.strptime(iso, _ISO).replace(tzinfo=_UTC)


def _fmt_utc(d: dt.datetime) -> str:
    return (d.astimezone(_UTC) if d.tzinfo else d).strftime(_ISO)

DDL = """CREATE TABLE IF NOT EXISTS {table} (
    day Date,
    tenant_id LowCardinality(String),
    entity_id String,
    window_start DateTime('UTC'),
    window_end DateTime('UTC'),
    n UInt64,
    mean_score Float64,
    min_score Float64,
    max_score Float64,
    stddev_score Float64,
    last_score Float64,
    last_ts Int64,
    signal_types Array(String),
    sources Array(String),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY day
ORDER BY (day, tenant_id, entity_id)"""

SELECT = "SELECT %s FROM {table} FINAL WHERE " % ", ".join(COLUMNS[:-1])
_KEY = "tenant_id = {tenant_id:String} AND entity_id = {entity_id:String}"
GET_SQL = SELECT + "day = {day:Date} AND " + _KEY
MGET_SQL = SELECT + "day IN {days:Array(Date)} AND " + _KEY
COUNT_SQL = "SELECT count() FROM {table} FINAL WHERE day = {day:Date}"
# Partition id of a Date key is YYYYMMDD; forcing the merge makes later FINAL reads free.
OPTIMIZE_SQL = "OPTIMIZE TABLE {table} PARTITION ID '{pid}' FINAL"
# lifecycle: a day partition is the unit of retirement, like an index on the OpenSearch side
PARTITIONS_SQL = "SELECT DISTINCT day FROM {table} ORDER BY day"
DROP_SQL = "ALTER TABLE {table} DROP PARTITION ID '{pid}'"


def doc_to_row(doc: Dict, updated_at: Optional[dt.datetime] = None) -> List:
    updated_at = updated_at or dt.datetime.now(_UTC)
    return [dt.date.fromisoformat(doc["day"]), doc["tenant_id"], doc["entity_id"],
            _parse_utc(doc["window_start"]), _parse_utc(doc["window_end"]),
            int(doc["n"]), float(doc["mean_score"]), float(doc["min_score"]), float(doc["max_score"]),
            float(doc["stddev_score"]), float(doc["last_score"]), int(doc["last_ts"]),
            list(doc.get("signal_types") or []), list(doc.get("sources") or []), updated_at]


def row_to_doc(row: Dict) -> Dict:
    """Back to the OpenSearch document shape so the API is sink-agnostic."""
    d = dict(row)
    d.pop("updated_at", None)
    d["day"] = d["day"].isoformat()
    d["window_start"] = _fmt_utc(d["window_start"])
    d["window_end"] = _fmt_utc(d["window_end"])
    return d


class ClickHouseStore:
    """`index` arguments keep the `prefix-YYYY-MM-DD` form; only the day part matters here."""

    def __init__(self, url: str = "http://localhost:8123", table: str = TABLE, client=None) -> None:
        if client is None:
            import clickhouse_connect

            u = urlparse(url)
            client = clickhouse_connect.get_client(host=u.hostname, port=u.port or 8123, interface=u.scheme,
                                                   username=u.username or "default", password=u.password or "",
                                                   database=u.path.strip("/") or "default")
        self.client = client
        self.table = table
        self._ready = False
        # str.replace, not str.format: `{day:Date}` is clickhouse-connect's server-side binding syntax
        self._sql = lambda tmpl: tmpl.replace("{table}", table)

    def ensure_index(self, index: str, shards: Optional[int] = None) -> None:
        if not self._ready:
            self.client.command(self._sql(DDL))
            self._ready = True

    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int:
        self.ensure_index(index)
        now = dt.datetime.now(_UTC)
        rows = [doc_to_row(d, now) for d in docs]
        if rows:
            self.client.insert(self.table, rows, column_names=COLUMNS)
        return len(rows)

    def _select(self, sql: str, params: Dict) -> List[Dict]:
        self.ensure_index("")
        res = self.client.query(self._sql(sql), parameters=params)
        return [row_to_doc(r) for r in res.named_results()]

    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]:
        tenant, entity = split_doc_id(doc_id)
        docs = self._select(GET_SQL, {"day": dt.date.fromisoformat(day_of(index)),
                                      "tenant_id": tenant, "entity_id": entity})
        return docs[0] if docs else None

    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]:
        tenant, entity = split_doc_id(doc_id)
        days = [dt.date.fromisoformat(day_of(i)) for i in indices]
        return self._select(MGET_SQL, {"days": days, "tenant_id": tenant, "entity_id": entity})

    def count(self, index: str) -> int:
        self.ensure_index(index)
        res = self.client.query(self._sql(COUNT_SQL),
                                parameters={"day": dt.date.fromisoformat(day_of(index))})
        return int(res.result_rows[0][0])

    def refresh(self, index: str) -> None:
        self.ensure_index(index)
        self.client.command(self._sql(OPTIMIZE_SQL).replace("{pid}", day_of(index).replace("-", "")))

    def list_indices(self, pattern: str) -> List[str]:
        """Partitions reported as `<pattern-prefix>-<day>` so the lifecycle step can parse them."""
        self.ensure_index("")
        prefix = pattern.rstrip("*").rstrip("-")
        res = self.client.query(self._sql(PARTITIONS_SQL))
        return ["%s-%s" % (prefix, r[0].isoformat()) for r in res.result_rows]

    def delete_index(self, index: str) -> None:
        self.client.command(self._sql(DROP_SQL).replace("{pid}", day_of(index).replace("-", "")))

    def alias_indices(self, alias: str) -> List[str]:
        return []  # no aliases: reads are `WHERE day IN ...` on one table

    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> None:
        pass


class InMemoryClickHouseStore:
    """Fake with ReplacingMergeTree semantics: inserts append, reads collapse to the newest row per key,
    `refresh` is the merge."""

    def __init__(self) -> None:
        self.rows: List[Tuple[str, str, float, Dict]] = []  # (day, doc_id, version, doc)
        self._ver = 0.0

    def ensure_index(self, index: str, shards: Optional[int] = None) -> None:
        pass

    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int:
        day = day_of(index)
        self._ver = max(self._ver + 1, time.time())
        n = 0
        for d in docs:
            self.rows.append((day, doc_key(d), self._ver, dict(d)))
            n += 1
        return n

    def _final(self, days: Optional[List[str]] = None) -> Dict[Tuple[str, str], Dict]:
        latest: Dict[Tuple[str, str], Tuple[float, Dict]] = {}
        for day, eid, ver, doc in self.rows:
            if (days is None or day in days) and ver >= latest.get((day, eid), (-1.0, None))[0]:
                latest[(day, eid)] = (ver, doc)
        return {k: v[1] for k, v in latest.items()}

    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]:
        return self._final([day_of(index)]).get((day_of(index), doc_id))

    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]:
        final = self._final([day_of(i) for i in indices])
        return [final[(day_of(i), doc_id)] for i in indices if (day_of(i), doc_id) in final]

    def count(self, index: str) -> int:
        return len(self._final([day_of(index)]))

    def refresh(self, index: str) -> None:
        day = day_of(index)
        merged = [(d, e, self._ver, doc) for (d, e), doc in self._final([day]).items()]
        self.rows = [r for r in self.rows if r[0] != day] + merged

    def list_indices(self, pattern: str) -> List[str]:
        prefix = pattern.rstrip("*").rstrip("-")
        return ["%s-%s" % (prefix, d) for d in sorted({r[0] for r in self.rows})]

    def delete_index(self, index: str) -> None:
        self.rows = [r for r in self.rows if r[0] != day_of(index)]

    def alias_indices(self, alias: str) -> List[str]:
        return []

    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> None:
        pass
