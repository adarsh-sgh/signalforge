"""Structural analysis of a submitted SQL statement, before it reaches Trino.

Everything the guard can decide without touching the cluster lives here: which tables a query
reads, which of their columns it actually *prunes* on, whether it projects `*` without a LIMIT,
and whether it contains a cross join. Parsing is sqlglot with the Trino dialect, so the same
grammar the cluster accepts.

"Constrained" deliberately means a sargable predicate -- a bare column compared to a literal
with `=`, `IN`, `BETWEEN` or an inequality. `WHERE day <> '2026-09-01'` or
`WHERE substr(day, 1, 7) = '2026-09'` reference the partition column but still read every
partition, so they do not count.
"""
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

import sqlglot
from sqlglot import exp

DIALECT = "trino"
SARGABLE = (exp.EQ, exp.In, exp.Between, exp.GT, exp.GTE, exp.LT, exp.LTE)
_KINDS = {exp.Select: "select", exp.Union: "select", exp.Insert: "insert", exp.Update: "update",
          exp.Delete: "delete", exp.Create: "ddl", exp.Drop: "ddl", exp.Alter: "ddl"}


class ParseError(ValueError):
    pass


@dataclass(frozen=True)
class TableRef:
    name: str
    schema: Optional[str] = None
    catalog: Optional[str] = None

    @property
    def qualified(self) -> str:
        return ".".join(p for p in (self.catalog, self.schema, self.name) if p)

    def matches(self, pattern: str) -> bool:
        """A policy may name a table fully (`delta.signals.signals_daily`) or by a suffix
        (`signals_daily`), so one policy covers the table however a query spells it."""
        want, got = pattern.lower().split("."), self.qualified.lower().split(".")
        return len(want) <= len(got) and got[-len(want):] == want


@dataclass(frozen=True)
class QueryShape:
    kind: str
    tables: Tuple[TableRef, ...]
    predicates: Dict[str, FrozenSet[str]]   # qualified table -> sargable column names
    has_limit: bool
    projects_star: bool
    joins: int
    cross_joins: int
    statements: int = 1

    def policy_for(self, patterns: Sequence[str]) -> List[Tuple[TableRef, str]]:
        return [(t, p) for t in self.tables for p in patterns if t.matches(p)]

    def constrained(self, table: TableRef, columns: Sequence[str]) -> bool:
        """True when at least one of `columns` is pruned on, which is all a partition filter needs."""
        got = self.predicates.get(table.qualified, frozenset())
        return any(c.lower() in got for c in columns)


def _table_ref(node: exp.Table) -> TableRef:
    return TableRef(node.name, node.text("db") or None, node.text("catalog") or None)


def _sargable_columns(root: exp.Expression) -> List[exp.Column]:
    """Columns compared to something constant inside a WHERE or a join condition."""
    out: List[exp.Column] = []
    scopes = list(root.find_all(exp.Where)) + [j.args["on"] for j in root.find_all(exp.Join)
                                               if j.args.get("on") is not None]
    for scope in scopes:
        for pred in scope.find_all(*SARGABLE):
            columns = [c for c in (pred.this, pred.args.get("expression")) if isinstance(c, exp.Column)]
            if isinstance(pred, exp.Between) and isinstance(pred.this, exp.Column):
                columns = [pred.this]
            if isinstance(pred, exp.In) and isinstance(pred.this, exp.Column):
                columns = [pred.this]
            out.extend(columns)
    return out


def _projects_star(root: exp.Expression) -> bool:
    """True only for a star in a projection list (`SELECT *`, `SELECT t.*`).

    `count(*)` also contains a Star node but reads one column's worth of nothing, so walking every
    Star in the tree would refuse the most common aggregate query there is.
    """
    for select in root.find_all(exp.Select):
        for projection in select.expressions:
            if isinstance(projection, exp.Star):
                return True
            if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
                return True
    return False


def _alias_map(root: exp.Expression) -> Dict[str, str]:
    """alias (or bare table name) -> qualified table name."""
    out: Dict[str, str] = {}
    for node in root.find_all(exp.Table):
        ref = _table_ref(node)
        out[node.alias_or_name.lower()] = ref.qualified
        out[ref.name.lower()] = ref.qualified
    return out


def analyze(sql: str) -> QueryShape:
    """Parse one statement. More than one statement is itself reportable: the guard rejects it
    rather than trying to reason about a batch."""
    try:
        parsed = [s for s in sqlglot.parse(sql, read=DIALECT) if s is not None]
    except Exception as e:                       # sqlglot raises several types
        raise ParseError(str(e))
    if not parsed:
        raise ParseError("empty statement")
    root = parsed[0]

    kind = "other"
    for node_type, name in _KINDS.items():
        if isinstance(root, node_type):
            kind = name
            break
    if isinstance(root, exp.Command) and root.this and root.this.lower() == "explain":
        kind = "explain"
    elif isinstance(root, exp.Describe):
        kind = "explain"

    tables = tuple(sorted({_table_ref(t) for t in root.find_all(exp.Table)}, key=lambda t: t.qualified))
    qualified = [t.qualified for t in tables]
    aliases = _alias_map(root)

    predicates: Dict[str, Set[str]] = {q: set() for q in qualified}
    for col in _sargable_columns(root):
        owner = aliases.get(col.table.lower()) if col.table else None
        # an unqualified column in a multi-table query could belong to any of them; counting it
        # for all of them keeps the guard from rejecting a query that does prune
        for target in ([owner] if owner else qualified):
            if target in predicates:
                predicates[target].add(col.name.lower())

    joins = len(list(root.find_all(exp.Join)))
    cross = sum(1 for j in root.find_all(exp.Join)
                if j.args.get("on") is None and j.args.get("using") is None)
    return QueryShape(kind=kind, tables=tables,
                      predicates={k: frozenset(v) for k, v in predicates.items()},
                      has_limit=root.args.get("limit") is not None,
                      projects_star=_projects_star(root),
                      joins=joins, cross_joins=cross, statements=len(parsed))
