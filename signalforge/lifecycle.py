"""Daily index lifecycle: pre-create the next day's indices, roll each read alias over the retention
window, delete indices older than it. Runs as the last task of the Airflow DAG; safe to re-run.

Creating tomorrow's index here (with the shard count the capacity model chose) keeps index creation
off the streaming write path at midnight. Aliases move in one atomic swap, so a reader on
`signals` or `signals-<tenant>` never sees a half-updated window.
"""
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from signalforge.metrics import INDICES_RETIRED
from signalforge.search.store import SearchStore
from signalforge.tenancy import DEFAULT_TENANT, Router


@dataclass
class Report:
    created: List[str] = field(default_factory=list)
    alias_added: Dict[str, List[str]] = field(default_factory=dict)
    alias_removed: Dict[str, List[str]] = field(default_factory=dict)
    deleted: List[str] = field(default_factory=list)


def _shift(day: str, n: int) -> str:
    return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()


def rollover(store: SearchStore, router: Router, today: str, retention_days: int,
             shards: Optional[Dict[str, int]] = None) -> Report:
    """Keep `retention_days` days ending today (plus tomorrow, pre-created); retire the rest."""
    if retention_days < 1:
        raise ValueError("retention_days must be >= 1")
    oldest_kept, tomorrow = _shift(today, 1 - retention_days), _shift(today, 1)
    shards = shards or {}
    rep = Report()
    for alias, tenant in router.families():
        t = tenant or DEFAULT_TENANT
        family: Dict[str, str] = {}  # index -> day, only this family's indices
        for idx in store.list_indices(alias + "-*"):
            parsed = router.parse_index(idx)
            if parsed and parsed[0] == tenant:
                family[idx] = parsed[1]
        for day in (today, tomorrow):
            idx = router.index_for(t, day)
            if idx not in family:
                store.ensure_index(idx, shards.get(tenant or "pooled"))
                family[idx] = day
                rep.created.append(idx)
        retire = sorted(i for i, d in family.items() if d < oldest_kept)
        wanted = set(family) - set(retire)
        current = set(store.alias_indices(alias))
        add, remove = sorted(wanted - current), sorted(current - wanted)
        if (add or remove) and store.update_alias(alias, add, remove):
            rep.alias_added[alias], rep.alias_removed[alias] = add, remove
        for idx in retire:  # after the alias swap, so a reader never hits a deleted member
            store.delete_index(idx)
            INDICES_RETIRED.labels(alias=alias).inc()
            rep.deleted.append(idx)
    return rep
