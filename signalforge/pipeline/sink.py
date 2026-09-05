"""Write aggregated documents into per-day indices, upserting by entity_id."""
from collections import defaultdict
from typing import Dict, Iterable

from pyspark.sql import DataFrame

from signalforge.metrics import DOCS_UPSERTED
from signalforge.search.store import SearchStore, index_name



def write_documents(store: SearchStore, prefix: str, docs: Iterable[Dict], chunk: int = 2000) -> int:
    """Group by day so each bulk request targets a single index."""
    by_index = defaultdict(list)
    written = 0
    for d in docs:
        idx = index_name(prefix, d["day"])
        by_index[idx].append(d)
        if len(by_index[idx]) >= chunk:
            written += _flush(store, idx, by_index.pop(idx))
    for idx, batch in by_index.items():
        written += _flush(store, idx, batch)
    return written


def _flush(store: SearchStore, idx: str, batch) -> int:
    n = store.bulk_upsert(idx, batch)
    DOCS_UPSERTED.labels(index=idx).inc(n)
    return n


def write_dataframe(store: SearchStore, prefix: str, docs: DataFrame) -> int:
    """Aggregates are small (one row per entity per window) so streaming them through the driver is fine."""
    return write_documents(store, prefix, (r.asDict() for r in docs.toLocalIterator()))
