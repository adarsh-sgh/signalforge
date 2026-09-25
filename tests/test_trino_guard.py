"""The guard, end to end: SQL in, verdict + audit record out. No Trino needed -- the byte
estimator is the only part that talks to a cluster and it is injected."""
import json

import pytest

from signalforge.trino.audit import InMemoryAudit, JsonlAudit, Outcome, open_audit
from signalforge.trino.guard import (ALLOW, GiB, REJECT, THROTTLE, Guard, Policy, TablePolicy, human,
                                     summarize)
from signalforge.trino.plan import ParseError, TableRef, analyze

TABLE = "delta.signals.signals_daily"
PRUNED = "SELECT entity_id, mean_score FROM %s WHERE day = '2026-09-03' AND tenant_id = 't-0'" % TABLE


def guard(estimate=None, **policy):
    kw = dict(tables={"signals_daily": TablePolicy(partition_columns=("day",))})
    kw.update(policy)
    audit = InMemoryAudit()
    return Guard(Policy(**kw), estimator=(lambda _sql: estimate), audit=audit), audit


def test_analyze_sees_tables_pruning_columns_and_shape():
    shape = analyze(PRUNED)
    assert shape.kind == "select" and not shape.projects_star and not shape.has_limit
    assert [t.qualified for t in shape.tables] == [TABLE]
    assert shape.predicates[TABLE] == frozenset({"day", "tenant_id"})
    assert shape.constrained(shape.tables[0], ["day"])

    # a policy may name the table by suffix or in full
    ref = TableRef("signals_daily", "signals", "delta")
    assert ref.matches("signals_daily") and ref.matches("signals.signals_daily") and ref.matches(TABLE)
    assert not ref.matches("other.signals_daily")

    # referencing the partition column without pruning on it does not count
    for sql in ("SELECT n FROM %s WHERE substr(day, 1, 7) = '2026-09'" % TABLE,
                "SELECT n FROM %s WHERE day <> '2026-09-03'" % TABLE,
                "SELECT n FROM %s" % TABLE):
        assert not analyze(sql).constrained(analyze(sql).tables[0], ["day"])
    # these do
    for sql in ("SELECT n FROM %s WHERE day IN ('2026-09-03')" % TABLE,
                "SELECT n FROM %s WHERE day BETWEEN '2026-09-01' AND '2026-09-03'" % TABLE,
                "SELECT n FROM %s WHERE day >= '2026-09-01'" % TABLE):
        assert analyze(sql).constrained(analyze(sql).tables[0], ["day"])

    joined = analyze("SELECT a.n FROM %s a JOIN delta.signals.tenants t ON a.tenant_id = t.id "
                     "WHERE a.day = '2026-09-03'" % TABLE)
    assert joined.joins == 1 and joined.cross_joins == 0
    assert joined.predicates[TABLE] == frozenset({"day", "tenant_id"})
    assert analyze("SELECT * FROM %s a, delta.signals.tenants t" % TABLE).cross_joins == 1
    with pytest.raises(ParseError):
        analyze("SELECT (")


def test_each_rule_rejects_the_query_it_is_there_for_and_lets_the_good_one_through():
    g, audit = guard(estimate=100 << 20)
    cases = {
        PRUNED: (ALLOW, "ok"),
        "SELECT n FROM %s" % TABLE: (REJECT, "missing_partition_predicate"),
        "SELECT * FROM %s WHERE day = '2026-09-03'" % TABLE: (REJECT, "unbounded_select_star"),
        "SELECT * FROM %s WHERE day = '2026-09-03' LIMIT 10" % TABLE: (ALLOW, "ok"),
        "SELECT a.n FROM %s a, delta.signals.tenants t WHERE a.day = '2026-09-03'" % TABLE:
            (REJECT, "cross_join"),
        "DROP TABLE %s" % TABLE: (REJECT, "statement_not_allowed"),
        "SELECT 1; SELECT 2": (REJECT, "multi_statement"),
        "SELECT (": (REJECT, "unparsable"),
        # a table with no policy is not partition-checked
        "SELECT * FROM delta.signals.tenants LIMIT 5": (ALLOW, "ok"),
    }
    for sql, (verdict, rule) in cases.items():
        d = g.admit("analyst", sql)
        assert (d.verdict, d.rule) == (verdict, rule), (sql, d.reason)
        g.release("analyst") if d.allowed else None
        assert d.reason and d.query_hash and d.sql == sql

    assert len(audit.decisions) == len(cases)
    got = summarize(audit.decisions)
    assert got["total"] == 9 and got["by_verdict"] == {ALLOW: 3, REJECT: 6}
    assert got["by_rule"]["missing_partition_predicate"] == 1


def test_scan_budget_uses_the_estimate_and_the_tightest_table_limit():
    over, audit = guard(estimate=9 * GiB, max_scan_bytes=5 * GiB)
    d = over.admit("analyst", PRUNED)
    assert d.verdict == REJECT and d.rule == "scan_budget_exceeded"
    assert d.estimated_bytes == 9 * GiB and d.budget_bytes == 5 * GiB
    assert "9.0 GiB" in d.reason and "5.0 GiB" in d.reason
    assert summarize(audit.decisions)["estimated_bytes_blocked"] == 9 * GiB

    under, _ = guard(estimate=1 * GiB, max_scan_bytes=5 * GiB)
    assert under.admit("analyst", PRUNED).verdict == ALLOW

    tight, _ = guard(estimate=2 * GiB, max_scan_bytes=5 * GiB,
                     tables={"signals_daily": TablePolicy(("day",), max_scan_bytes=1 * GiB)})
    assert tight.admit("analyst", PRUNED).rule == "scan_budget_exceeded"

    # no estimator, or an estimator that blows up, must never turn into a rejection
    blind = Guard(Policy.default(), estimator=None)
    assert blind.admit("analyst", PRUNED).verdict == ALLOW
    broken = Guard(Policy.default(), estimator=lambda _s: 1 / 0)
    assert broken.admit("analyst", PRUNED).verdict == ALLOW


def test_per_user_concurrency_throttles_and_recovers_and_users_are_independent():
    g, audit = guard(max_concurrent_per_user=2)
    assert [g.admit("a", PRUNED).verdict for _ in range(2)] == [ALLOW, ALLOW]
    third = g.admit("a", PRUNED)
    assert third.verdict == THROTTLE and third.rule == "user_concurrency_cap"
    assert third.retry_after_seconds == 5 and third.inflight == 2
    assert g.admit("b", PRUNED).verdict == ALLOW      # another user has their own budget
    g.release("a")
    assert g.admit("a", PRUNED).verdict == ALLOW and g.admitted("a") == 2
    for _ in range(5):
        g.release("a")
    assert g.admitted("a") == 0
    assert summarize(audit.decisions)["by_verdict"][THROTTLE] == 1


def test_policy_round_trips_through_json_and_the_audit_log_is_replayable(tmp_path):
    raw = json.dumps({"tables": {"signals_daily": {"partition_columns": ["day"],
                                                   "max_scan_bytes": 1073741824}},
                      "max_scan_bytes": 5368709120, "max_concurrent_per_user": 4,
                      "allowed_kinds": ["select"]})
    policy = Policy.from_json(raw)
    assert policy.tables["signals_daily"].partition_columns == ("day",)
    assert policy.max_concurrent_per_user == 4 and policy.allowed_kinds == ("select",)

    path = str(tmp_path / "audit" / "decisions.jsonl")
    audit = open_audit(path)
    assert isinstance(audit, JsonlAudit) and isinstance(open_audit(None), InMemoryAudit)
    g = Guard(policy, estimator=lambda _s: 2 << 20, audit=audit)
    allowed = g.admit("analyst", PRUNED)
    refused = g.admit("analyst", "SELECT n FROM %s" % TABLE)
    audit.record_outcome(Outcome(allowed.query_hash, "20260925_000_x", "FINISHED",
                                 processed_bytes=1 << 20, processed_rows=42, wall_ms=310))

    lines = audit.read_all()
    assert [line["record"] for line in lines] == ["decision", "decision", "outcome"]
    assert lines[1]["rule"] == "missing_partition_predicate" and lines[1]["verdict"] == REJECT
    assert lines[1]["tables"] == [TABLE] and lines[2]["processed_rows"] == 42
    summary = audit.summary()
    assert summary["by_verdict"] == {ALLOW: 1, REJECT: 1} and summary["outcomes"] == 1
    assert summary["actual_bytes_scanned"] == 1 << 20
    assert [d["query_hash"] for d in audit.tail(1)] == [refused.query_hash]
    assert human(1536) == "1.5 KiB" and human(None) == "unknown"
