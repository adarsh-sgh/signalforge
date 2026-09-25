"""The admission service over a fake coordinator: allowed queries are forwarded and their outcome
recorded, refused ones never reach Trino, throttled ones come back with Retry-After."""
import json

import pytest
from fastapi.testclient import TestClient

from signalforge.config import Settings
from signalforge.trino.audit import InMemoryAudit
from signalforge.trino.client import FakeTrino, QueryResult, scan_bytes_from_io_plan
from signalforge.trino.guard import GiB, Policy, TablePolicy
from signalforge.trino.service import create_app, load_policy

TABLE = "delta.signals.signals_daily"
PRUNED = "SELECT entity_id, mean_score FROM %s WHERE day = '2026-09-03'" % TABLE
WIDE = "SELECT entity_id FROM %s" % TABLE


def client(estimate=1 << 20, rows=None, **policy):
    kw = dict(tables={"signals_daily": TablePolicy(partition_columns=("day",))}, max_scan_bytes=1 * GiB)
    kw.update(policy)
    fake = FakeTrino(default_estimate=estimate,
                     result=QueryResult(("entity_id", "mean_score"), rows if rows is not None else
                                        [["ent-1", 3.5], ["ent-2", 4.0]], processed_bytes=2 << 20))
    audit = InMemoryAudit()
    return TestClient(create_app(fake, Policy(**kw), audit)), fake, audit


def test_allowed_query_is_forwarded_and_its_outcome_recorded():
    c, fake, audit = client()
    body = c.post("/v1/queries", json={"sql": PRUNED, "user": "analyst"}).json()
    assert body["decision"]["verdict"] == "allow" and body["decision"]["user"] == "analyst"
    assert body["columns"] == ["entity_id", "mean_score"] and body["rows"][0] == ["ent-1", 3.5]
    assert body["stats"]["processed_bytes"] == 2 << 20 and body["stats"]["query_id"] == "fake_2"
    # the coordinator saw the EXPLAIN (for the estimate) and then the statement itself
    assert [sql.startswith("EXPLAIN") for _u, sql in fake.executed] == [True, False]
    assert [u for u, _ in fake.executed][-1] == "analyst"

    summary = c.get("/v1/summary").json()
    assert summary["by_verdict"] == {"allow": 1} and summary["actual_bytes_scanned"] == 2 << 20
    assert audit.outcomes[0].state == "FINISHED" and audit.outcomes[0].processed_rows == 2
    # the slot was released, so the same user can keep going
    assert c.post("/v1/queries", json={"sql": PRUNED, "user": "analyst"}).status_code == 200
    assert c.get("/healthz").json() == {"ok": True, "trino": True}


def test_refused_query_is_403_with_the_rule_and_never_reaches_the_coordinator():
    c, fake, audit = client()
    r = c.post("/v1/queries", json={"sql": WIDE, "user": "analyst"})
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert detail["rule"] == "missing_partition_predicate" and detail["verdict"] == "reject"
    assert detail["tables"] == [TABLE] and detail["estimated_bytes"] == 1 << 20
    assert [sql.startswith("EXPLAIN") for _u, sql in fake.executed] == [True]   # only the estimate ran

    over = c.post("/v1/queries", json={"sql": "SELECT * FROM %s WHERE day = '2026-09-03'" % TABLE})
    assert over.status_code == 403 and over.json()["detail"]["rule"] == "unbounded_select_star"
    assert c.get("/v1/summary").json()["by_verdict"] == {"reject": 2}
    assert len(audit.outcomes) == 0
    assert [d["rule"] for d in c.get("/v1/decisions", params={"limit": 1}).json()["decisions"]] \
        == ["unbounded_select_star"]


def test_scan_budget_and_concurrency_cap_are_visible_over_http():
    c, _fake, _audit = client(estimate=4 * GiB, max_scan_bytes=1 * GiB)
    r = c.post("/v1/queries", json={"sql": PRUNED})
    assert r.status_code == 403 and r.json()["detail"]["rule"] == "scan_budget_exceeded"
    assert r.json()["detail"]["budget_bytes"] == 1 * GiB

    # the cap is only reachable while a query is in flight, so drive the guard directly
    capped, _f, _a = client(max_concurrent_per_user=1)
    guard = capped.app.state.guard
    guard.admit("hog", PRUNED)                       # takes the one slot and holds it
    r = capped.post("/v1/queries", json={"sql": PRUNED}, headers={"X-Trino-User": "hog"})
    assert r.status_code == 429 and r.headers["retry-after"] == "5"
    assert r.json()["detail"]["rule"] == "user_concurrency_cap"
    guard.release("hog")
    assert capped.post("/v1/queries", json={"sql": PRUNED},
                       headers={"X-Trino-User": "hog"}).status_code == 200


def test_a_query_that_fails_inside_trino_is_502_and_frees_the_slot():
    c, fake, audit = client(max_concurrent_per_user=1)
    ok = c.post("/v1/queries", json={"sql": PRUNED, "user": "analyst"})
    assert ok.status_code == 200
    fake.fail_with = RuntimeError("TABLE_NOT_FOUND")
    bad = c.post("/v1/queries", json={"sql": PRUNED, "user": "analyst"})
    assert bad.status_code == 502 and "TABLE_NOT_FOUND" in bad.json()["detail"]["error"]
    assert audit.outcomes[-1].state == "FAILED"
    assert c.app.state.guard.admitted("analyst") == 0     # released in the finally
    fake.fail_with = None
    assert c.post("/v1/queries", json={"sql": PRUNED, "user": "analyst"}).status_code == 200


def test_policy_is_served_and_can_come_from_a_file(tmp_path):
    c, _f, _a = client()
    served = c.get("/v1/policy").json()
    assert served["tables"]["signals_daily"]["partition_columns"] == ["day"]
    assert served["allowed_kinds"] == ["select", "explain"]

    path = tmp_path / "policy.json"
    path.write_text(json.dumps({"tables": {"signals_daily": {"partition_columns": ["day"]}},
                                "max_scan_bytes": 123456, "max_concurrent_per_user": 9}))
    policy = load_policy(Settings(trino_policy_file=str(path)))
    assert policy.max_scan_bytes == 123456 and policy.max_concurrent_per_user == 9
    # without a file the policy comes from env-backed settings
    env = load_policy(Settings(trino_max_scan_bytes=777, trino_max_concurrent_per_user=2,
                               lake_table="signals_daily"))
    assert env.max_scan_bytes == 777 and env.tables["signals_daily"].partition_columns == ("day",)


def test_io_explain_plan_is_summed_and_missing_statistics_mean_no_estimate():
    plan = {"inputTableColumnInfos": [
        {"table": {"catalog": "delta"}, "estimate": {"outputSizeInBytes": 1048576.0}},
        {"table": {"catalog": "delta"}, "estimate": {"outputSizeInBytes": 2097152.0}}]}
    assert scan_bytes_from_io_plan(plan) == 3 << 20
    assert scan_bytes_from_io_plan({"inputTableColumnInfos": [
        {"estimate": {"outputSizeInBytes": float("nan")}}]}) is None
    assert scan_bytes_from_io_plan({}) is None
    assert scan_bytes_from_io_plan({"inputTableColumnInfos": [
        {"estimate": {"outputSizeInBytes": float("nan")}},
        {"estimate": {"outputSizeInBytes": 4096.0}}]}) == 4096
