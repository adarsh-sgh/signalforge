"""Freshness SLOs end to end: probe readings in, breaches and healing actions out, including the
bounded-retry behaviour and a real backfill through the batch job."""
import os
import time

from signalforge.config import Settings
from signalforge.search.store import InMemoryStore
from signalforge.sla import (BACKFILL, DEFERRED, ESCALATE, INCOMPLETE, MISSING, NONE, STALE, Breach,
                             Freshness, Healer, Slo, archive_freshness, check, slos_from_settings,
                             store_freshness, sweep)
from tests.conftest import D1, D2

NOW = 1_790_000_000_000   # epoch ms
SLOS = [Slo("archive", max_lag_seconds=3600, min_rows=1), Slo("lake", max_lag_seconds=7200, min_rows=100),
        Slo("serving", max_lag_seconds=3600, heal=NONE)]


def ago(seconds):
    return NOW - seconds * 1000


def test_check_classifies_missing_stale_incomplete_and_healthy():
    readings = [
        Freshness("archive", D1, None, 0),                       # nothing written at all
        Freshness("lake", D1, ago(10_000), 5_000),               # 10000s old, budget 7200
        Freshness("serving", D1, ago(60), 1_000),                # fresh
        Freshness("unknown", D1, None, 0),                       # no SLO -> not our problem
    ]
    breaches = check(readings, SLOS, now_ms=NOW)
    assert [(b.dataset, b.kind) for b in breaches] == [("archive", MISSING), ("lake", STALE)]
    assert breaches[1].lag_seconds == 10_000 and breaches[1].budget_seconds == 7200
    assert "10000s old, budget 7200s" in breaches[1].describe()
    assert "nothing for %s" % D1 in breaches[0].describe()

    # a fresh but short day is incomplete, not stale
    short = check([Freshness("lake", D1, ago(60), 42)], SLOS, now_ms=NOW)
    assert [(b.kind, b.rows, b.min_rows) for b in short] == [(INCOMPLETE, 42, 100)]
    assert check([Freshness("lake", D1, ago(60), 500)], SLOS, now_ms=NOW) == []


def test_healer_retries_with_backoff_then_escalates_and_resets_when_healthy():
    clock = [1000.0]
    healed = []
    healer = Healer([Slo("archive", 3600, max_attempts=2, backoff_seconds=60)],
                    {BACKFILL: lambda b: healed.append(b.day) or 7}, clock=lambda: clock[0])
    breach = Breach("archive", D1, MISSING, 3600)

    first = healer.heal_one(breach)
    assert (first.action, first.attempt, first.ok) == (BACKFILL, 1, True)
    assert first.detail == "rebuilt 7 rows" and healed == [D1]

    immediate = healer.heal_one(breach)                  # still inside the backoff window
    assert (immediate.action, immediate.ok) == (DEFERRED, True) and healed == [D1]

    clock[0] += 61
    second = healer.heal_one(breach)
    assert (second.action, second.attempt) == (BACKFILL, 2) and healed == [D1, D1]

    clock[0] += 61
    third = healer.heal_one(breach)
    assert (third.action, third.ok) == (ESCALATE, False) and "2 attempts exhausted" in third.detail
    assert healed == [D1, D1]                            # never tried a third time

    # a later healthy sweep clears the counter, so tomorrow starts from attempt 1
    report = sweep([Freshness("archive", D1, clock[0] * 1000 - 1000, 3)], [Slo("archive", 3600)],
                   healer, now_ms=int(clock[0] * 1000))
    assert report["breaches"] == [] and report["healed"] == 0
    clock[0] += 61
    assert healer.heal_one(breach).attempt == 1


def test_a_failing_handler_is_one_failed_attempt_not_a_crashed_sweep():
    def explode(_breach):
        raise RuntimeError("s3 unreachable")

    healer = Healer([Slo("lake", 3600), Slo("archive", 3600)],
                    {BACKFILL: lambda b: explode(b) if b.dataset == "lake" else 5},
                    clock=lambda: 0.0)
    report = sweep([Freshness("lake", D1, None), Freshness("archive", D1, None)],
                   [Slo("lake", 3600), Slo("archive", 3600)], healer, now_ms=NOW)
    actions = {a["dataset"]: a for a in report["actions"]}
    assert actions["lake"]["ok"] is False and "s3 unreachable" in actions["lake"]["detail"]
    assert actions["archive"]["ok"] is True and report["healed"] == 1
    assert len(report["breaches"]) == 2 and report["escalated"] == []


def test_probes_read_the_real_archive_and_serving_store(cfg, archive, spark):
    """The archive probe is the streaming job's own output directory; the serving probe counts the
    day's indices. Then a breach on a missing day heals by re-running the batch job for real."""
    from signalforge.pipeline.job import run_batch
    from signalforge.tenancy import Router

    fresh = archive_freshness(cfg.archive_dir, D1)
    assert fresh.dataset == "archive" and fresh.rows == 1 and fresh.updated_at_ms is not None
    assert fresh.lag_seconds(int(time.time() * 1000)) < 120
    assert archive_freshness(cfg.archive_dir, "2020-01-01").updated_at_ms is None

    store = InMemoryStore()
    indices = Router.from_settings(cfg).day_indices(D1)
    assert store_freshness(store, indices, D1).updated_at_ms is None    # nothing indexed yet

    healer = Healer([Slo("serving", 3600, min_rows=2)],
                    {BACKFILL: lambda b: run_batch(spark, cfg.archive_dir, store, cfg, day=b.day)})
    report = sweep([store_freshness(store, indices, D1)], [Slo("serving", 3600, min_rows=2)],
                   healer, now_ms=NOW)
    assert [b["kind"] for b in report["breaches"]] == [MISSING]
    assert report["actions"][0]["detail"] == "rebuilt 2 rows" and report["healed"] == 1

    after = store_freshness(store, indices, D1, updated_at_ms=NOW)
    assert after.rows == 2
    assert check([after], [Slo("serving", 3600, min_rows=2)], now_ms=NOW) == []


def test_slos_come_from_one_env_string():
    slos = slos_from_settings(Settings(sla="archive:900,lake:3600:1000,serving:600:0:none"))
    assert [(s.dataset, s.max_lag_seconds, s.min_rows, s.heal) for s in slos] == [
        ("archive", 900, 0, BACKFILL), ("lake", 3600, 1000, BACKFILL), ("serving", 600, 0, NONE)]
    assert slos_from_settings(Settings(sla="")) == []
    # a dataset whose SLO says not to heal is reported, never repaired
    healer = Healer(slos, {BACKFILL: lambda b: 1}, clock=lambda: 0.0)
    action = healer.heal_one(Breach("serving", D2, MISSING, 600))
    assert action.action == NONE and action.ok is True
