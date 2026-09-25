"""Admission control for Trino: decide, before a statement is submitted, whether to run it,
make its owner wait, or refuse it.

Rules, cheapest first, so an obviously bad query never costs a coordinator round trip:

  unparsable / multi_statement   the guard will not reason about what it cannot parse
  statement_not_allowed          only SELECT / EXPLAIN by default; writes go through the pipeline
  missing_partition_predicate    a table declared partitioned must be pruned on
  cross_join                     a join with no condition
  unbounded_select_star          `SELECT *` with no LIMIT
  scan_budget_exceeded           Trino's own IO estimate over the byte budget
  user_concurrency_cap           throttled, not refused: retry after the hint

The byte estimate comes from `EXPLAIN (TYPE IO, FORMAT JSON)` (see `client.estimate_scan_bytes`),
which is an estimate and can be absent; a missing estimate never blocks a query, it just means
that rule did not fire. Every decision, including allows, goes to the audit log.
"""
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from signalforge.metrics import TRINO_DECISIONS, TRINO_INFLIGHT, TRINO_SCAN_BLOCKED
from signalforge.trino.plan import ParseError, QueryShape, TableRef, analyze

ALLOW, THROTTLE, REJECT = "allow", "throttle", "reject"
GiB = 1 << 30

Estimator = Callable[[str], Optional[int]]


@dataclass(frozen=True)
class TablePolicy:
    partition_columns: Tuple[str, ...] = ()
    max_scan_bytes: Optional[int] = None   # overrides the global budget for this table


@dataclass(frozen=True)
class Policy:
    tables: Dict[str, TablePolicy] = field(default_factory=dict)
    max_scan_bytes: int = 5 * GiB
    max_concurrent_per_user: int = 3
    require_limit_on_star: bool = True
    allow_cross_join: bool = False
    allowed_kinds: Tuple[str, ...] = ("select", "explain")
    retry_after_seconds: int = 5

    @classmethod
    def default(cls, lake_table: str = "signals_daily") -> "Policy":
        """The lake table as the pipeline writes it: one partition column, `day`."""
        return cls(tables={lake_table: TablePolicy(partition_columns=("day",))})

    @classmethod
    def from_json(cls, raw: str) -> "Policy":
        d = json.loads(raw)
        tables = {name: TablePolicy(tuple(t.get("partition_columns", ())), t.get("max_scan_bytes"))
                  for name, t in (d.pop("tables", None) or {}).items()}
        if "allowed_kinds" in d:
            d["allowed_kinds"] = tuple(d["allowed_kinds"])
        return cls(tables=tables, **d)

    def budget_for(self, shape: QueryShape) -> int:
        """Tightest budget among the tables the query touches."""
        limits = [self.tables[p].max_scan_bytes for _, p in shape.policy_for(list(self.tables))
                  if self.tables[p].max_scan_bytes is not None]
        return min(limits + [self.max_scan_bytes])


@dataclass(frozen=True)
class Decision:
    verdict: str
    rule: str
    reason: str
    user: str
    query_hash: str
    sql: str
    at_ms: int
    estimated_bytes: Optional[int] = None
    budget_bytes: Optional[int] = None
    inflight: int = 0
    retry_after_seconds: Optional[int] = None
    tables: Tuple[str, ...] = ()

    @property
    def allowed(self) -> bool:
        return self.verdict == ALLOW

    def to_dict(self) -> Dict:
        d = dict(self.__dict__)
        d["tables"] = list(self.tables)
        return d


def query_hash(sql: str) -> str:
    return hashlib.sha1(" ".join(sql.split()).encode()).hexdigest()[:16]


class Guard:
    """Stateful only in the per-user in-flight counters; the rules themselves are pure."""

    def __init__(self, policy: Optional[Policy] = None, estimator: Optional[Estimator] = None,
                 audit=None, clock: Callable[[], float] = time.time) -> None:
        self.policy = policy or Policy.default()
        self.estimator = estimator
        self.audit = audit
        self.clock = clock
        self.inflight: Dict[str, int] = {}

    # -- rules ---------------------------------------------------------------
    def _structural(self, shape: QueryShape) -> Optional[Tuple[str, str]]:
        if shape.statements > 1:
            return "multi_statement", "%d statements in one submission" % shape.statements
        if shape.kind not in self.policy.allowed_kinds:
            return "statement_not_allowed", "%s is not in %s" % (shape.kind, list(self.policy.allowed_kinds))
        for table, pattern in shape.policy_for(list(self.policy.tables)):
            columns = self.policy.tables[pattern].partition_columns
            if columns and not shape.constrained(table, columns):
                return ("missing_partition_predicate",
                        "%s is partitioned by %s and the query prunes on none of them"
                        % (table.qualified, ", ".join(columns)))
        if shape.cross_joins and not self.policy.allow_cross_join:
            return "cross_join", "%d join(s) with no condition" % shape.cross_joins
        if self.policy.require_limit_on_star and shape.projects_star and not shape.has_limit:
            return "unbounded_select_star", "SELECT * without a LIMIT"
        return None

    def _estimate(self, sql: str) -> Optional[int]:
        if self.estimator is None:
            return None
        try:
            return self.estimator(sql)
        except Exception:
            return None      # the coordinator being unreachable must not become a rejection

    # -- decision ------------------------------------------------------------
    def decide(self, user: str, sql: str) -> Decision:
        """Pure in the sense that it takes no slot; `admit` is the one that reserves capacity."""
        now = int(self.clock() * 1000)
        base = dict(user=user, query_hash=query_hash(sql), sql=sql, at_ms=now,
                    inflight=self.inflight.get(user, 0))
        try:
            shape = analyze(sql)
        except ParseError as e:
            return Decision(REJECT, "unparsable", str(e), **base)
        base["tables"] = tuple(t.qualified for t in shape.tables)
        # the estimate is taken before the structural rules so a refusal can be *costed*: the
        # audit then says how much of the lake the rejected statement would have read. EXPLAIN
        # (TYPE IO) is planning only, so this never reads data.
        budget = self.policy.budget_for(shape)
        estimate = self._estimate(sql) if shape.kind in self.policy.allowed_kinds else None
        base.update(estimated_bytes=estimate, budget_bytes=budget)

        hit = self._structural(shape)
        if hit:
            return Decision(REJECT, hit[0], hit[1], **base)

        if estimate is not None and estimate > budget:
            return Decision(REJECT, "scan_budget_exceeded",
                            "estimated scan %s over the %s budget" % (human(estimate), human(budget)),
                            **base)

        cap = self.policy.max_concurrent_per_user
        if cap and base["inflight"] >= cap:
            return Decision(THROTTLE, "user_concurrency_cap",
                            "%s already has %d queries running (cap %d)" % (user, base["inflight"], cap),
                            retry_after_seconds=self.policy.retry_after_seconds, **base)
        return Decision(ALLOW, "ok", "admitted", **base)

    def admit(self, user: str, sql: str) -> Decision:
        """Decide, record, and reserve a slot when the verdict is allow."""
        decision = self.decide(user, sql)
        if decision.allowed:
            self.inflight[user] = self.inflight.get(user, 0) + 1
            TRINO_INFLIGHT.labels(user=user).set(self.inflight[user])
        elif decision.verdict == REJECT and decision.estimated_bytes:
            TRINO_SCAN_BLOCKED.inc(decision.estimated_bytes)
        TRINO_DECISIONS.labels(verdict=decision.verdict, rule=decision.rule).inc()
        if self.audit is not None:
            self.audit.record(decision)
        return decision

    def release(self, user: str) -> None:
        left = max(0, self.inflight.get(user, 0) - 1)
        self.inflight[user] = left
        TRINO_INFLIGHT.labels(user=user).set(left)

    def admitted(self, user: str) -> int:
        return self.inflight.get(user, 0)


def human(n: Optional[int]) -> str:
    if n is None:
        return "unknown"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%d B" % n


def summarize(decisions: Sequence[Decision]) -> Dict:
    """What the guard actually bought: counts per verdict/rule and the estimated bytes refused."""
    by_verdict: Dict[str, int] = {}
    by_rule: Dict[str, int] = {}
    blocked = admitted_bytes = 0
    for d in decisions:
        by_verdict[d.verdict] = by_verdict.get(d.verdict, 0) + 1
        by_rule[d.rule] = by_rule.get(d.rule, 0) + 1
        if d.verdict == REJECT:
            blocked += d.estimated_bytes or 0
        elif d.allowed:
            admitted_bytes += d.estimated_bytes or 0
    return {"total": len(decisions), "by_verdict": by_verdict, "by_rule": by_rule,
            "estimated_bytes_blocked": blocked, "estimated_bytes_admitted": admitted_bytes}
