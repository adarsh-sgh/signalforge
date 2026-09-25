"""End-to-end runs of the PyFlink anomaly job on a local MiniCluster.

Needs apache-flink (requirements-flink.txt), which only has x86_64 Linux wheels, so these
skip on an arm64 laptop and run in `make flink-test` (linux/amd64 container) and in CI.
"""
import json

import pytest

pytest.importorskip("pyflink", reason="apache-flink not installed (requirements-flink.txt)")

from pyflink.common import Types, WatermarkStrategy  # noqa: E402

from signalforge.anomaly import Detector, DetectorConfig, window_stats  # noqa: E402
from signalforge.config import Settings  # noqa: E402
from signalforge.events.codec import encode  # noqa: E402
from signalforge.flink import anomaly_job as job  # noqa: E402

W = 60_000
T0 = 1_788_393_600_000  # 2026-09-03T00:00:00Z


def events(n_quiet=20, spike=9.0, per_window=4, entity="ent-1", tenant="t-0"):
    """`n_quiet` quiet minutes then one minute at `spike`, `per_window` events each."""
    out = []
    for w in range(n_quiet + 1):
        score = spike if w == n_quiet else (3.5 + 0.1 * (w % 2))
        for i in range(per_window):
            out.append({"tenant_id": tenant, "entity_id": entity, "score": score,
                        "ts": T0 + w * W + i * 1000})
    return out


def rows(evs):
    return [(e["tenant_id"], e["entity_id"], e["score"], e["ts"]) for e in evs]


def cfg(**kw):
    kw.setdefault("anomaly_window_ms", W)
    kw.setdefault("anomaly_min_samples", 5)
    kw.setdefault("anomaly_min_stddev", 0.01)
    return Settings(**kw)


def run(evs, settings, detector=None):
    """Run the graph on a local MiniCluster and split the tagged anomaly / late output."""
    env = job.build_env(1)
    src = env.from_collection(rows(evs), type_info=job.EVENT_TYPE)
    _stats, anomalies, late = job.anomaly_pipeline(src, settings, detector)
    tagged = (anomalies.map(lambda s: "anomaly " + s, output_type=Types.STRING())
              .union(late.map(lambda r: "late %s" % (r,), output_type=Types.STRING())))
    out = list(tagged.execute_and_collect())
    return ([json.loads(o[len("anomaly "):]) for o in out if o.startswith("anomaly ")],
            [o for o in out if o.startswith("late ")])


def test_flink_job_finds_the_same_anomalies_as_the_pure_python_detector():
    """The window operator plus keyed detector state must agree with `window_stats` + `Detector`,
    which is what the tests of the detector itself pin down."""
    evs = events()
    settings = cfg(anomaly_threshold=3.0)
    got, late = run(evs, settings)
    want = Detector(DetectorConfig.from_settings(settings)).run(window_stats(evs, W))

    assert late == []                       # every event is inside its window's watermark
    assert len(got) == 1, got
    assert [a["window_start"] for a in got] == [w.window_start for w in want]
    assert got[0]["window_start"] == T0 + 20 * W and got[0]["window_end"] == T0 + 21 * W
    assert got[0]["value"] == 9.0 and got[0]["n"] == 4
    assert got[0]["score"] == pytest.approx(want[0].score, rel=1e-6)
    assert got[0]["baseline"] == pytest.approx(want[0].baseline, rel=1e-6)
    assert got[0]["detector"] == "zscore" and got[0]["tenant_id"] == "t-0"


def test_threshold_and_detector_mode_change_what_the_job_emits():
    """Same input, three configurations: a threshold high enough to swallow the bump, a low one
    that catches it, and the EWMA detector instead of the rolling z-score."""
    evs = events(n_quiet=25, spike=4.6)
    assert run(evs, cfg(anomaly_threshold=50.0))[0] == []
    loose, _ = run(evs, cfg(anomaly_threshold=2.0))
    assert len(loose) == 1 and loose[0]["threshold"] == 2.0 and loose[0]["value"] == 4.6
    ewma, _ = run(evs, cfg(anomaly_mode="ewma", anomaly_threshold=2.0))
    assert [a["detector"] for a in ewma] == ["ewma"]


def test_keyed_state_is_per_entity_and_per_tenant():
    """Four keys over the same windows, one of them spiking: keyed detector state must not leak."""
    evs = (events(entity="ent-1")
           + [dict(e, entity_id="ent-2", score=3.5) for e in events(entity="ent-1")]
           + [dict(e, tenant_id="t-1") for e in events(entity="ent-1")]
           + [dict(e, tenant_id="t-1", entity_id="ent-2", score=3.5) for e in events(entity="ent-1")])
    got, _ = run(evs, cfg(anomaly_threshold=3.0))
    assert sorted((a["tenant_id"], a["entity_id"]) for a in got) == [("t-0", "ent-1"), ("t-1", "ent-1")]


def test_protobuf_bytes_from_kafka_decode_into_the_event_stream():
    payloads = [(encode(entity_id=e["entity_id"], signal_type="review", score=e["score"], source="web",
                        ts=e["ts"], tenant_id=e["tenant_id"]),) for e in events()]
    payloads.append((b"garbage",))
    env = job.build_env(1)
    raw = env.from_collection(payloads, type_info=Types.TUPLE([Types.PRIMITIVE_ARRAY(Types.BYTE())]))
    decoded = job.decode_events(raw.map(lambda t: t[0], output_type=Types.PRIMITIVE_ARRAY(Types.BYTE())))
    _stats, anomalies, _late = job.anomaly_pipeline(decoded, cfg(anomaly_threshold=3.0))
    got = [json.loads(s) for s in anomalies.execute_and_collect()]
    assert len(got) == 1 and got[0]["value"] == 9.0   # the poison payload never reached a window
    assert got[0]["tenant_id"] == "t-0" and got[0]["entity_id"] == "ent-1"


def test_window_bounds_and_late_stream_wiring():
    """`allowed_lateness(0)` plus a side output means a closed window is never re-fired: every
    window in the output is distinct and aligned to the window size. (Whether a straggler actually
    reaches the late stream depends on watermark progress, which a bounded collection source never
    makes mid-stream; scripts/smoke.sh checks that leg against Redpanda.)"""
    evs = events(n_quiet=12, per_window=3)
    got, late = run(evs, cfg(anomaly_threshold=3.0))
    assert late == []
    starts = [a["window_start"] for a in got]
    assert len(starts) == len(set(starts))
    assert all((s - T0) % W == 0 for s in starts)
    assert all(a["window_end"] - a["window_start"] == W for a in got)
