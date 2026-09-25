import math

import pytest

from signalforge.anomaly import (EWMA, Anomaly, Detector, DetectorConfig, DetectorState, WindowStat,
                                 window_stats)
from tests.conftest import EVENTS

W = 60_000


def stat(i, mean, n=10, tenant="t", entity="e"):
    return WindowStat(tenant, entity, i * W, (i + 1) * W, n, mean * n, mean - 0.5, mean + 0.5)


def test_zscore_warms_up_then_flags_a_spike_and_absorbs_the_new_level():
    """20 quiet windows, one spike, then the spike level repeats until it becomes normal again."""
    d = Detector(DetectorConfig(threshold=3.0, min_samples=5, history=10, min_stddev=0.01))
    quiet = [stat(i, 3.5 + (0.1 if i % 2 else -0.1)) for i in range(20)]
    assert d.run(quiet) == []                      # noise inside the band, and the warm-up window
    assert len(d.states["t:e"].recent) == 10       # rolling baseline is bounded by `history`

    spike = d.observe(stat(20, 9.0))
    assert isinstance(spike, Anomaly)
    assert spike.window_start == 20 * W and spike.window_end == 21 * W
    assert spike.value == 9.0 and abs(spike.baseline - 3.5) < 0.01
    assert spike.score > 3.0 and spike.metric == "mean_score" and spike.detector == "zscore"
    assert spike.samples == 20
    # the spike itself widens the band, so a sustained shift alerts once instead of every window
    assert not any(d.observe(stat(21 + i, 9.0)) is not None for i in range(12))
    # once 9.0 is the whole baseline, dropping back to the old level is the anomaly
    assert d.observe(stat(40, 3.5)) is not None


def test_ewma_tracks_a_drift_without_alerting_but_still_catches_a_jump():
    """A slow ramp is absorbed by the smoothed mean; a step change is not."""
    cfg = DetectorConfig(mode=EWMA, alpha=0.3, threshold=3.0, min_samples=5, min_stddev=0.05)
    d = Detector(cfg)
    assert d.run([stat(i, 3.5 + 0.02 * i) for i in range(40)]) == []
    st = d.states["t:e"]
    assert st.samples == 40 and 3.5 < st.mean < 4.3 and math.sqrt(st.var) < 0.1
    jump = d.observe(stat(40, st.mean + 5.0))
    assert jump is not None and jump.detector == EWMA and jump.score > 3.0
    # state survives a json round trip (this is what Flink keeps in ValueState)
    assert DetectorState.from_json(st.to_json()) == st


def test_threshold_is_the_only_knob_between_quiet_and_noisy():
    stats = [stat(i, 3.5) for i in range(10)] + [stat(10, 3.5 + 0.3), stat(11, 3.5 + 1.0)]
    strict = Detector(DetectorConfig(threshold=6.0, min_samples=5, min_stddev=0.1))
    loose = Detector(DetectorConfig(threshold=2.0, min_samples=5, min_stddev=0.1))
    assert len(strict.run(stats)) == 1 and len(loose.run(stats)) == 2
    # with_threshold keeps the baseline and only re-scores: the same quiet window is an
    # anomaly at 0.1 and not at 2.0
    assert loose.with_threshold(2.0).run([stat(12, 3.5)]) == []
    assert [round(a.score, 2) for a in loose.with_threshold(0.1).run([stat(12, 3.5)])] == [-0.39]


def test_event_rate_metric_is_volume_not_score_and_scales_with_window_length():
    d = Detector(DetectorConfig(metric="event_rate", threshold=3.0, min_samples=5, min_stddev=0.001))
    assert d.run([stat(i, 3.5, n=10) for i in range(10)]) == []
    hit = d.observe(stat(10, 3.5, n=600))
    assert hit is not None and hit.metric == "event_rate"
    assert hit.value == pytest.approx(10.0) and hit.baseline == pytest.approx(10 / 60)
    assert hit.n == 600


def test_window_stats_are_the_tumbling_aggregate_the_flink_job_computes():
    stats = window_stats([dict(e, tenant_id=e.get("tenant_id", "default")) for e in EVENTS], W)
    # 7 events, one an exact redelivery, over 6 distinct (entity, minute) windows
    assert len(stats) == 6
    assert [s.window_start % W for s in stats] == [0] * 6            # aligned to the window size
    assert all(s.window_end - s.window_start == W for s in stats)
    first = next(s for s in stats if s.entity_id == "ent-1" and s.n > 1)
    assert first.n == 2 and first.sum_score == 8.0 and first.mean_score == 4.0
    assert first.min_score == 4.0 and first.max_score == 4.0
    merged = stats[0].merge(stats[0])
    assert merged.n == stats[0].n * 2 and merged.window_start == stats[0].window_start


def test_config_rejects_nonsense():
    for bad in ({"mode": "magic"}, {"metric": "vibes"}, {"threshold": 0}, {"history": 1}, {"alpha": 0}):
        with pytest.raises(ValueError):
            DetectorConfig(**bad)
