import datetime as dt

from fastapi.testclient import TestClient

from signalforge.api.app import create_app
from signalforge.clickhouse_store import (COLUMNS, ClickHouseStore, InMemoryClickHouseStore, doc_to_row,
                                          row_to_doc)
from signalforge.pipeline.job import run_batch
from tests.conftest import D1, D2


def test_batch_into_clickhouse_fake_and_read_via_api(spark, cfg, archive):
    store = InMemoryClickHouseStore()
    assert run_batch(spark, archive, store, cfg) == 4
    assert run_batch(spark, archive, store, cfg, day=D1) == 2  # replay: rows appended, not overwritten
    assert len(store.rows) == 6 and store.count("test-%s" % D1) == 2  # FINAL collapses per (day, entity)
    store.refresh("test-%s" % D1)  # merge
    assert len(store.rows) == 4

    c = TestClient(create_app(store, prefix="test"))
    r = c.get("/entities/ent-1", params={"day": D1})
    assert r.status_code == 200 and r.json()["n"] == 3 and r.json()["mean_score"] == 3.6667
    assert c.get("/entities/ent-9", params={"day": D1}).status_code == 404
    h = c.get("/entities/ent-1/history", params={"end": D2, "days": 7}).json()
    assert [d["day"] for d in h["daily"]] == [D1, D2] and h["events"] == 4


class _Client:
    """Records the SQL a real clickhouse-connect client would receive; echoes inserted rows on query."""

    def __init__(self):
        self.commands, self.inserts, self.queries = [], [], []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def insert(self, table, data, column_names):
        self.inserts.append((table, data, column_names))

    def query(self, sql, parameters=None):
        self.queries.append((sql, parameters))
        rows = [dict(zip(cols, r)) for _, data, cols in self.inserts for r in data]
        days = parameters.get("days", [parameters.get("day")])
        rows = [r for r in rows if r["day"] in days and r["entity_id"] == parameters.get("entity_id", r["entity_id"])]

        class Result:
            result_rows = [(len(rows),)]

            def named_results(self):
                return iter(rows)

        return Result()


def test_generated_sql_and_row_roundtrip():
    client = _Client()
    store = ClickHouseStore(client=client)
    doc = {"entity_id": "ent-1", "day": D1, "window_start": D1 + "T00:00:00Z", "window_end": D2 + "T00:00:00Z",
           "n": 3, "mean_score": 3.6667, "min_score": 2.0, "max_score": 5.0, "stddev_score": 1.2472,
           "last_score": 5.0, "last_ts": 1788400800000, "signal_types": ["review", "rating"], "sources": ["web"]}

    assert store.bulk_upsert("test-%s" % D1, [doc]) == 1
    store.bulk_upsert("test-%s" % D1, [])  # empty micro-batch: no insert
    ddl = client.commands[0]
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS signals_daily")
    assert "ENGINE = ReplacingMergeTree(updated_at)" in ddl
    assert "PARTITION BY day" in ddl and "ORDER BY (day, entity_id)" in ddl
    assert len(client.commands) == 1  # DDL once per store
    table, rows, cols = client.inserts[0]
    assert table == "signals_daily" and cols == COLUMNS and len(client.inserts) == 1
    assert rows[0][0] == dt.date.fromisoformat(D1) and rows[0][2] == dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc)
    assert isinstance(rows[0][-1], dt.datetime)  # version column drives ReplacingMergeTree

    assert store.get("test-%s" % D1, "ent-1") == doc
    assert store.get("test-%s" % D1, "ent-9") is None
    assert store.mget(["test-%s" % D1, "test-%s" % D2], "ent-1") == [doc]
    assert store.count("test-%s" % D1) == 1
    store.refresh("test-%s" % D1)
    get_sql, get_params = client.queries[0]
    assert get_sql.startswith("SELECT day, entity_id, ") and " FROM signals_daily FINAL WHERE " in get_sql
    assert "day = {day:Date} AND entity_id = {entity_id:String}" in get_sql
    assert get_params == {"day": dt.date.fromisoformat(D1), "entity_id": "ent-1"}
    assert "day IN {days:Array(Date)}" in client.queries[2][0]
    assert client.queries[3][0] == "SELECT count() FROM signals_daily FINAL WHERE day = {day:Date}"
    assert client.commands[1] == "OPTIMIZE TABLE signals_daily PARTITION ID '20260903' FINAL"
    assert row_to_doc(dict(zip(COLUMNS, doc_to_row(doc)))) == doc
