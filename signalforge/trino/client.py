"""Thin Trino client: run a statement, and ask the coordinator what a statement would read.

`estimate_scan_bytes` is the guard's byte oracle. `EXPLAIN (TYPE IO, FORMAT JSON)` returns, per
input table, the constraints the planner pushed down and a cost estimate; summing
`estimate.outputSizeInBytes` over the input tables is the planner's own answer to "how much of
the lake does this touch", which is exactly the question a scan budget asks. It is an estimate:
it is missing (NaN) when the connector has no statistics, and `FakeTrino` stands in for tests.
"""
import json
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from signalforge.config import Settings, settings

IO_EXPLAIN = "EXPLAIN (TYPE IO, FORMAT JSON) %s"


@dataclass
class QueryResult:
    columns: Tuple[str, ...]
    rows: List[Sequence[Any]]
    query_id: str = ""
    state: str = "FINISHED"
    processed_bytes: Optional[int] = None
    processed_rows: Optional[int] = None
    wall_ms: Optional[int] = None

    def dicts(self) -> List[Dict]:
        return [dict(zip(self.columns, r)) for r in self.rows]


def _finite(value) -> Optional[int]:
    """IO estimates come back as floats and are NaN when the connector has no statistics."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if math.isfinite(number) else None


def scan_bytes_from_io_plan(plan: Dict) -> Optional[int]:
    """Sum the planner's per-input-table size estimates; None when none of them is known."""
    total, known = 0, False
    for info in plan.get("inputTableColumnInfos") or []:
        size = _finite((info.get("estimate") or {}).get("outputSizeInBytes"))
        if size is not None:
            total += size
            known = True
    return total if known else None


class TrinoClient:
    def __init__(self, cfg: Settings = settings, user: str = "signalforge") -> None:
        self.cfg = cfg
        self.user = user

    def _connect(self, user: Optional[str] = None):
        import trino

        from urllib.parse import urlparse

        u = urlparse(self.cfg.trino_url)
        return trino.dbapi.connect(host=u.hostname, port=u.port or 8080, user=user or self.user,
                                   catalog=self.cfg.trino_catalog, schema=self.cfg.trino_schema,
                                   http_scheme=u.scheme or "http")

    def run(self, sql: str, user: Optional[str] = None) -> QueryResult:
        conn = self._connect(user)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = tuple(d[0] for d in (cur.description or []))
            stats = cur.stats or {}
            return QueryResult(columns, rows, query_id=stats.get("queryId", ""),
                               state=stats.get("state", "FINISHED"),
                               processed_bytes=stats.get("processedBytes"),
                               processed_rows=stats.get("processedRows"),
                               wall_ms=stats.get("elapsedTimeMillis"))
        finally:
            conn.close()

    def estimate_scan_bytes(self, sql: str) -> Optional[int]:
        result = self.run(IO_EXPLAIN % sql)
        if not result.rows:
            return None
        return scan_bytes_from_io_plan(json.loads(result.rows[0][0]))

    def health(self) -> bool:
        try:
            return self.run("SELECT 1").rows[0][0] == 1
        except Exception:
            return False


@dataclass
class FakeTrino:
    """In-memory stand-in with the same surface: canned results by SQL prefix, canned estimates.
    Lets the service and the workload driver be tested without a coordinator."""
    estimates: Dict[str, Optional[int]] = field(default_factory=dict)
    default_estimate: Optional[int] = None
    result: QueryResult = field(default_factory=lambda: QueryResult(("n",), [[1]]))
    executed: List[Tuple[str, str]] = field(default_factory=list)
    fail_with: Optional[Exception] = None

    def run(self, sql: str, user: Optional[str] = None) -> QueryResult:
        self.executed.append((user or "", sql))
        if self.fail_with is not None:
            raise self.fail_with
        return QueryResult(self.result.columns, list(self.result.rows),
                           query_id="fake_%d" % len(self.executed), state="FINISHED",
                           processed_bytes=self.result.processed_bytes,
                           processed_rows=len(self.result.rows), wall_ms=7)

    def estimate_scan_bytes(self, sql: str) -> Optional[int]:
        # recorded like the real client, which asks the coordinator to plan an EXPLAIN (TYPE IO)
        self.executed.append(("", IO_EXPLAIN % sql))
        for prefix, value in self.estimates.items():
            if prefix in sql:
                return value
        return self.default_estimate

    def health(self) -> bool:
        return True
