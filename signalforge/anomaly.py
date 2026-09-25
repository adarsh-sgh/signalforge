"""Statistical anomaly detection over windowed aggregates.

Pure functions plus a small state record, so the same detector runs inside a Flink
keyed operator (state in `ValueState`), in a batch replay and in tests. Two modes:

- `zscore`: baseline is the mean/stddev of the last `history` closed windows.
- `ewma`:   baseline is an exponentially weighted mean with an EWMA variance, so
            it needs O(1) state per key and forgets old regime changes faster.

Both compare the window's metric against the baseline built from *earlier* windows
only, so a window is never judged against itself and the first `min_samples`
windows of a key are warm-up. Every window is folded into the baseline afterwards,
anomalous or not, so a sustained shift alerts once and then becomes the new normal
instead of producing an alert per window; returning to the old level alerts again.
"""
import json
import math
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Tuple

ZSCORE = "zscore"
EWMA = "ewma"
MODES = (ZSCORE, EWMA)
METRICS = ("mean_score", "event_rate")


@dataclass(frozen=True)
class DetectorConfig:
    mode: str = ZSCORE
    metric: str = "mean_score"   # which window number to watch
    threshold: float = 3.0       # |deviation| in baseline stddevs before it is an anomaly
    min_samples: int = 5         # closed windows needed before the baseline is trusted
    history: int = 30            # zscore: windows kept in the rolling baseline
    alpha: float = 0.3           # ewma: smoothing factor for both mean and variance
    min_stddev: float = 0.05     # floor so a flat series does not make every wobble infinite

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError("mode must be one of %s" % (MODES,))
        if self.metric not in METRICS:
            raise ValueError("metric must be one of %s" % (METRICS,))
        if self.threshold <= 0 or self.min_samples < 1 or self.history < 2:
            raise ValueError("threshold > 0, min_samples >= 1, history >= 2")
        if not 0 < self.alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")

    @classmethod
    def from_settings(cls, cfg) -> "DetectorConfig":
        return cls(mode=cfg.anomaly_mode, metric=cfg.anomaly_metric, threshold=cfg.anomaly_threshold,
                   min_samples=cfg.anomaly_min_samples, history=cfg.anomaly_history,
                   alpha=cfg.anomaly_alpha, min_stddev=cfg.anomaly_min_stddev)


@dataclass(frozen=True)
class WindowStat:
    """One closed (tenant, entity, window) aggregate: what the Flink window operator emits."""
    tenant_id: str
    entity_id: str
    window_start: int   # epoch ms, inclusive
    window_end: int     # epoch ms, exclusive
    n: int
    sum_score: float
    min_score: float
    max_score: float

    @property
    def mean_score(self) -> float:
        return self.sum_score / self.n if self.n else 0.0

    def event_rate(self) -> float:
        """Events per second, so the metric does not change meaning when the window size does."""
        secs = max(1, self.window_end - self.window_start) / 1000.0
        return self.n / secs

    def value(self, metric: str) -> float:
        return self.mean_score if metric == "mean_score" else self.event_rate()

    def merge(self, other: "WindowStat") -> "WindowStat":
        return WindowStat(self.tenant_id, self.entity_id, min(self.window_start, other.window_start),
                          max(self.window_end, other.window_end), self.n + other.n,
                          self.sum_score + other.sum_score,
                          min(self.min_score, other.min_score), max(self.max_score, other.max_score))

    def key(self) -> str:
        return "%s:%s" % (self.tenant_id, self.entity_id)


@dataclass(frozen=True)
class DetectorState:
    """Per-key baseline. Picklable and JSON-able so it can live in Flink keyed state."""
    samples: int = 0
    recent: Tuple[float, ...] = ()   # zscore: the last `history` window values
    mean: float = 0.0                # ewma: smoothed mean
    var: float = 0.0                 # ewma: smoothed variance

    def to_json(self) -> str:
        return json.dumps({"samples": self.samples, "recent": list(self.recent),
                           "mean": self.mean, "var": self.var})

    @classmethod
    def from_json(cls, raw: str) -> "DetectorState":
        d = json.loads(raw)
        return cls(d["samples"], tuple(d["recent"]), d["mean"], d["var"])


@dataclass(frozen=True)
class Anomaly:
    tenant_id: str
    entity_id: str
    window_start: int
    window_end: int
    metric: str
    value: float
    baseline: float
    stddev: float
    score: float          # signed deviation in stddevs
    threshold: float
    detector: str
    n: int
    samples: int          # windows the baseline was built from

    def to_json(self) -> str:
        d = dict(self.__dict__)
        for k in ("value", "baseline", "stddev", "score"):
            d[k] = round(d[k], 6)
        return json.dumps(d, sort_keys=True)


def _baseline(state: DetectorState, cfg: DetectorConfig) -> Tuple[float, float]:
    """(mean, stddev) of the windows seen before this one."""
    if cfg.mode == EWMA:
        return state.mean, math.sqrt(max(state.var, 0.0))
    n = len(state.recent)
    mean = sum(state.recent) / n
    var = sum((x - mean) ** 2 for x in state.recent) / n   # population sd, like the Spark rollup
    return mean, math.sqrt(var)


def _advance(state: DetectorState, value: float, cfg: DetectorConfig) -> DetectorState:
    if cfg.mode == EWMA:
        if state.samples == 0:
            return DetectorState(samples=1, mean=value, var=0.0)
        dev = value - state.mean
        # West's incremental EWMA variance: update the variance with the *pre-update* deviation.
        var = (1 - cfg.alpha) * (state.var + cfg.alpha * dev * dev)
        return DetectorState(samples=state.samples + 1, mean=state.mean + cfg.alpha * dev, var=var)
    recent = (state.recent + (value,))[-cfg.history:]
    return DetectorState(samples=state.samples + 1, recent=recent)


def update(state: DetectorState, stat: WindowStat,
           cfg: DetectorConfig = DetectorConfig()) -> Tuple[DetectorState, Optional[Anomaly]]:
    """Score one closed window against the baseline, then fold it into the baseline.

    The window is always folded in, anomalous or not: a persistent shift should become the new
    normal after `history` (zscore) or ~`1/alpha` (ewma) windows rather than alerting forever.
    """
    value = stat.value(cfg.metric)
    if state.samples < cfg.min_samples:
        return _advance(state, value, cfg), None
    mean, sd = _baseline(state, cfg)
    sd = max(sd, cfg.min_stddev)
    z = (value - mean) / sd
    hit = None
    if abs(z) >= cfg.threshold:
        hit = Anomaly(stat.tenant_id, stat.entity_id, stat.window_start, stat.window_end, cfg.metric,
                      value, mean, sd, z, cfg.threshold, cfg.mode, stat.n, state.samples)
    return _advance(state, value, cfg), hit


@dataclass
class Detector:
    """Stateful wrapper for non-Flink callers (batch replay, tests, the Ray/serving path)."""
    cfg: DetectorConfig = DetectorConfig()
    states: Dict[str, DetectorState] = field(default_factory=dict)

    def observe(self, stat: WindowStat) -> Optional[Anomaly]:
        key = stat.key()
        state, hit = update(self.states.get(key, DetectorState()), stat, self.cfg)
        self.states[key] = state
        return hit

    def run(self, stats) -> List[Anomaly]:
        """Windows must arrive in event-time order per key, as they do out of a Flink window operator."""
        return [a for a in (self.observe(s) for s in stats) if a is not None]

    def with_threshold(self, threshold: float) -> "Detector":
        """Re-score the same stream at a different threshold without losing the baseline."""
        return Detector(replace(self.cfg, threshold=threshold), dict(self.states))


def window_stats(events, window_ms: int) -> List[WindowStat]:
    """Reference implementation of the tumbling event-time window the Flink job runs.

    Events are dicts as `codec.decode` returns them. Output is ordered by (key, window start),
    which is the order a keyed Flink window operator emits per key; the tests assert the Flink
    job produces the same anomalies as `Detector.run(window_stats(...))`.
    """
    acc: Dict[Tuple[str, str, int], WindowStat] = {}
    for e in events:
        start = (e["ts"] // window_ms) * window_ms
        tenant = e.get("tenant_id") or "default"
        key = (tenant, e["entity_id"], start)
        stat = WindowStat(tenant, e["entity_id"], start, start + window_ms, 1,
                          e["score"], e["score"], e["score"])
        acc[key] = stat if key not in acc else acc[key].merge(stat)
    return [acc[k] for k in sorted(acc)]
