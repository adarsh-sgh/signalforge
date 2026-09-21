"""Pick the rollup store from settings. Shared by the Spark job and the API."""
from typing import Optional

from signalforge.config import Settings, settings
from signalforge.search.store import SearchStore, open_store

SINKS = ("opensearch", "clickhouse")


def open_sink(cfg: Settings = settings, kind: Optional[str] = None) -> SearchStore:
    kind = kind or cfg.sink
    if kind == "clickhouse":
        from signalforge.clickhouse_store import ClickHouseStore

        store: SearchStore = ClickHouseStore(cfg.clickhouse_url)
    elif kind == "opensearch":
        store = open_store(cfg.opensearch_url, cfg.index_prefix)
    else:
        raise ValueError("unknown sink %r, expected one of %s" % (kind, SINKS))
    return store
