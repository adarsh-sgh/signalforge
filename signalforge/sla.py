"""Per-dataset freshness SLOs, breach detection, and the self-healing step that follows.

Each dataset the pipeline owns gets a budget: how old its newest write may be (`max_lag_seconds`)
and how many rows a finished day must have (`min_rows`). A probe reports the dataset's current
state, `check` turns probes plus SLOs into breaches, and `Healer` decides what to do about each
one -- normally re-running the day's backfill, which is idempotent on every sink (upsert by id on
OpenSearch, ReplacingMergeTree on ClickHouse, `replaceWhere` on Delta), so healing the same day
twice costs time and nothing else.

Healing is bounded on purpose: `max_attempts` per (dataset, day) with a backoff between tries, then
the breach is escalated rather than retried forever. A pipeline that silently retries a
permanently broken day is worse than one that pages someone.
"""
import glob
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from signalforge.metrics import DATASET_LAG, HEAL_ACTIONS, SLA_BREACHES

MISSING, STALE, INCOMPLETE = "missing", "stale", "incomplete"
BACKFILL, ESCALATE, DEFERRED, NONE = "backfill", "escalate", "deferred", "none"


@dataclass(frozen=True)
class Slo:
    dataset: str
    max_lag_seconds: int
    min_rows: int = 0
    heal: str = BACKFILL
    max_attempts: int = 2
    backoff_seconds: int = 60


@dataclass(frozen=True)
class Freshness:
    """What a probe reports: when the dataset was last written for `day`, and how much is there."""
    dataset: str
    day: str
    updated_at_ms: Optional[int] = None   # None: nothing has been written for this day at all
    rows: int = 0
    detail: str = ""

    def lag_seconds(self, now_ms: int) -> Optional[int]:
        return None if self.updated_at_ms is None else max(0, (now_ms - self.updated_at_ms) // 1000)


@dataclass(frozen=True)
class Breach:
    dataset: str
    day: str
    kind: str
    budget_seconds: int
    lag_seconds: Optional[int] = None
    rows: int = 0
    min_rows: int = 0
    detail: str = ""

    def describe(self) -> str:
        if self.kind == MISSING:
            return "%s has nothing for %s" % (self.dataset, self.day)
        if self.kind == STALE:
            return ("%s for %s is %ds old, budget %ds"
                    % (self.dataset, self.day, self.lag_seconds, self.budget_seconds))
        return ("%s for %s has %d rows, expected at least %d"
                % (self.dataset, self.day, self.rows, self.min_rows))


@dataclass(frozen=True)
class HealAction:
    dataset: str
    day: str
    action: str
    attempt: int
    ok: bool = True
    detail: str = ""


def check(readings: Sequence[Freshness], slos: Sequence[Slo], now_ms: Optional[int] = None) -> List[Breach]:
    """One breach per (dataset, day) at most: missing beats stale beats incomplete, because a
    missing day is not usefully also 'stale' and healing is the same action either way."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    by_dataset = {s.dataset: s for s in slos}
    out = []
    for reading in readings:
        slo = by_dataset.get(reading.dataset)
        if slo is None:
            continue
        lag = reading.lag_seconds(now_ms)
        DATASET_LAG.labels(dataset=reading.dataset).set(-1 if lag is None else lag)
        kind = None
        if reading.updated_at_ms is None:
            kind = MISSING
        elif lag is not None and lag > slo.max_lag_seconds:
            kind = STALE
        elif slo.min_rows and reading.rows < slo.min_rows:
            kind = INCOMPLETE
        if kind:
            SLA_BREACHES.labels(dataset=reading.dataset, kind=kind).inc()
            out.append(Breach(reading.dataset, reading.day, kind, slo.max_lag_seconds, lag,
                              reading.rows, slo.min_rows, reading.detail))
    return out


class Healer:
    """Runs the repair for a breach and remembers how often it has tried.

    `handlers` maps an SLO's `heal` name to a callable that does the work and returns how many rows
    it produced; anything it raises is captured as a failed attempt, so one broken dataset does not
    abort the rest of the sweep.
    """

    def __init__(self, slos: Sequence[Slo], handlers: Dict[str, Callable[[Breach], int]],
                 clock: Callable[[], float] = time.time) -> None:
        self.slos = {s.dataset: s for s in slos}
        self.handlers = handlers
        self.clock = clock
        self.attempts: Dict[Tuple[str, str], int] = {}
        self.last_try: Dict[Tuple[str, str], float] = {}

    def heal(self, breaches: Sequence[Breach]) -> List[HealAction]:
        return [self.heal_one(b) for b in breaches]

    def heal_one(self, breach: Breach) -> HealAction:
        key = (breach.dataset, breach.day)
        slo = self.slos[breach.dataset]
        attempt = self.attempts.get(key, 0) + 1
        if slo.heal == NONE or slo.heal not in self.handlers:
            return self._done(breach, NONE, attempt - 1, True, "no handler for %r" % slo.heal)
        if attempt > slo.max_attempts:
            return self._done(breach, ESCALATE, attempt - 1, False,
                              "%d attempts exhausted: %s" % (slo.max_attempts, breach.describe()))
        waited = self.clock() - self.last_try.get(key, 0.0)
        if key in self.last_try and waited < slo.backoff_seconds:
            return self._done(breach, DEFERRED, attempt - 1, True,
                              "backing off %ds more" % int(slo.backoff_seconds - waited))
        self.attempts[key], self.last_try[key] = attempt, self.clock()
        try:
            rows = self.handlers[slo.heal](breach)
        except Exception as e:
            return self._done(breach, slo.heal, attempt, False, "%s: %s" % (type(e).__name__, e))
        return self._done(breach, slo.heal, attempt, True, "rebuilt %d rows" % rows)

    def _done(self, breach: Breach, action: str, attempt: int, ok: bool, detail: str) -> HealAction:
        HEAL_ACTIONS.labels(dataset=breach.dataset, action=action).inc()
        return HealAction(breach.dataset, breach.day, action, attempt, ok, detail)

    def forget(self, dataset: str, day: str) -> None:
        """Called once a day is healthy again, so tomorrow's breach starts from attempt 1."""
        self.attempts.pop((dataset, day), None)
        self.last_try.pop((dataset, day), None)


def sweep(readings: Sequence[Freshness], slos: Sequence[Slo], healer: Healer,
          now_ms: Optional[int] = None) -> Dict:
    """Detect, heal, and report -- the shape the Airflow task pushes to XCom."""
    breaches = check(readings, slos, now_ms)
    breached = {(b.dataset, b.day) for b in breaches}
    for reading in readings:
        if (reading.dataset, reading.day) not in breached:
            healer.forget(reading.dataset, reading.day)
    actions = healer.heal(breaches)
    return {"checked": len(readings),
            "breaches": [dict(b.__dict__, describe=b.describe()) for b in breaches],
            "actions": [dict(a.__dict__) for a in actions],
            "healed": sum(1 for a in actions if a.ok and a.action not in (NONE, DEFERRED)),
            "escalated": [a.detail for a in actions if a.action == ESCALATE]}


# -- probes ------------------------------------------------------------------

def archive_freshness(archive_dir: str, day: str, dataset: str = "archive") -> Freshness:
    """Newest Parquet file in the day partition the streaming job appends to."""
    files = glob.glob(os.path.join(archive_dir, "day=%s" % day, "*.parquet"))
    if not files:
        return Freshness(dataset, day, None, 0, "no day=%s partition under %s" % (day, archive_dir))
    newest = max(os.path.getmtime(f) for f in files)
    return Freshness(dataset, day, int(newest * 1000), len(files), "%d files" % len(files))


def lake_freshness(lake, spark, day: str, dataset: str = "lake") -> Freshness:
    """Newest Delta commit. The commit log is the dataset's own write timestamp, so this needs no
    side table: if the nightly replay never ran, the newest commit is yesterday's."""
    commit = lake.latest(spark)
    if commit is None:
        return Freshness(dataset, day, None, 0, "no delta table at %s" % lake.path)
    rows = commit.rows if commit.rows is not None else 0
    return Freshness(dataset, day, commit.timestamp_ms, rows, "version %d" % commit.version)


def store_freshness(store, indices: Sequence[str], day: str, updated_at_ms: Optional[int] = None,
                    dataset: str = "serving") -> Freshness:
    """Row count of the serving store for one day across the pooled and dedicated indices.
    `updated_at_ms` is passed in by the caller that just wrote (the DAG's verify step), because
    the store protocol exposes counts, not write times."""
    rows = 0
    for index in indices:
        store.refresh(index)
        rows += store.count(index)
    return Freshness(dataset, day, updated_at_ms if rows else None, rows, "%d indices" % len(indices))


def slos_from_settings(cfg) -> List[Slo]:
    """`SF_SLA=archive:900,lake:3600:1000,serving:3600` -> dataset : max lag s [: min rows]."""
    out = []
    for part in filter(None, (p.strip() for p in cfg.sla.split(","))):
        fields = part.split(":")
        out.append(Slo(fields[0], int(fields[1]), int(fields[2]) if len(fields) > 2 else 0,
                       heal=fields[3] if len(fields) > 3 else BACKFILL))
    return out
