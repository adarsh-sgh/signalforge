"""Write aggregated documents into per-day indices, upserting by (tenant, entity)."""
from collections import defaultdict
from typing import Dict, Iterable, Optional, Tuple

from pyspark.sql import DataFrame

from signalforge.metrics import DOCS_UPSERTED
from signalforge.search.store import SearchStore
from signalforge.tenancy import Quota, Router


def write_documents(store: SearchStore, router: Router, docs: Iterable[Dict], quota: Optional[Quota] = None,
                    chunk: int = 2000) -> int:
    """Group by (index, routing) so each bulk request targets one index and one shard."""
    by_target: Dict[Tuple[str, Optional[str]], list] = defaultdict(list)
    index_of: Dict[Tuple[str, str], str] = {}  # (tenant, day) -> index; a few hundred keys, hit per document
    written = 0
    for d in docs:
        tenant, day = d["tenant_id"], d["day"]
        if quota is not None and not quota.admit(tenant, day, d["entity_id"]):
            continue
        idx = index_of.get((tenant, day))
        if idx is None:
            idx = index_of[(tenant, day)] = router.index_for(tenant, day)
        target = (idx, router.routing_for(tenant, d["entity_id"]))
        by_target[target].append(d)
        if len(by_target[target]) >= chunk:
            written += _flush(store, target, by_target.pop(target))
    for target, batch in by_target.items():
        written += _flush(store, target, batch)
    return written


def _flush(store: SearchStore, target: Tuple[str, Optional[str]], batch) -> int:
    idx, routing = target
    n = store.bulk_upsert(idx, batch, routing=routing)
    DOCS_UPSERTED.labels(index=idx).inc(n)
    return n


def write_dataframe(store: SearchStore, router: Router, docs: DataFrame, quota: Optional[Quota] = None) -> int:
    """Aggregates are small (one row per entity per window) so streaming them through the driver is fine."""
    return write_documents(store, router, (r.asDict() for r in docs.toLocalIterator()), quota)
