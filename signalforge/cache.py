"""Read-through cache for the API's point lookup, behind a protocol so tests run on a dict.

`CachedStore` wraps any `SearchStore`: `get` consults the cache first, `bulk_upsert` evicts the
entity-days it just wrote so a lookup after a micro-batch never returns the previous rollup.
"""
import json
import time
from typing import Callable, Dict, Iterable, List, Optional, Protocol

from signalforge.metrics import CACHE_LOOKUPS
from signalforge.search.store import SearchStore, day_of, doc_key


def cache_key(doc_id: str, day: str) -> str:
    """doc_id is `tenant:entity`, so keys are tenant-scoped."""
    return "entity:%s:%s" % (doc_id, day)


class Cache(Protocol):
    def get(self, key: str) -> Optional[Dict]: ...
    def set(self, key: str, doc: Dict, ttl: int) -> None: ...
    def delete(self, keys: Iterable[str]) -> None: ...


class InMemoryCache:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.data: Dict[str, tuple] = {}  # key -> (expires_at, doc)

    def get(self, key: str) -> Optional[Dict]:
        hit = self.data.get(key)
        if hit is None:
            return None
        if hit[0] <= self.clock():
            del self.data[key]
            return None
        return dict(hit[1])

    def set(self, key: str, doc: Dict, ttl: int) -> None:
        self.data[key] = (self.clock() + ttl, dict(doc))

    def delete(self, keys: Iterable[str]) -> None:
        for k in keys:
            self.data.pop(k, None)


class RedisCache:
    """JSON documents with a server-side TTL (`SET ... EX`)."""

    def __init__(self, url: str, client=None) -> None:
        if client is None:
            import redis

            client = redis.Redis.from_url(url)
        self.client = client

    def get(self, key: str) -> Optional[Dict]:
        raw = self.client.get(key)
        return json.loads(raw) if raw else None

    def set(self, key: str, doc: Dict, ttl: int) -> None:
        self.client.set(key, json.dumps(doc), ex=ttl)

    def delete(self, keys: Iterable[str], chunk: int = 1000) -> None:
        keys = list(keys)
        for i in range(0, len(keys), chunk):
            self.client.delete(*keys[i:i + chunk])


class CachedStore:
    def __init__(self, store: SearchStore, cache: Cache, ttl: int = 60) -> None:
        self.store, self.cache, self.ttl = store, cache, ttl

    def ensure_index(self, index: str, shards: Optional[int] = None) -> None:
        self.store.ensure_index(index, shards)

    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int:
        docs = list(docs)
        n = self.store.bulk_upsert(index, docs, routing=routing)
        # Evict rather than write through: most rollups are never read before the next batch replaces them.
        self.cache.delete(cache_key(doc_key(d), day_of(index)) for d in docs)
        return n

    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]:
        key = cache_key(doc_id, day_of(index))
        doc = self.cache.get(key)
        if doc is not None:
            CACHE_LOOKUPS.labels(result="hit").inc()
            return doc
        CACHE_LOOKUPS.labels(result="miss").inc()
        doc = self.store.get(index, doc_id, routing)
        if doc is not None:
            self.cache.set(key, doc, self.ttl)
        return doc

    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]:
        return self.store.mget(indices, doc_id, routing)

    def count(self, index: str) -> int:
        return self.store.count(index)

    def refresh(self, index: str) -> None:
        self.store.refresh(index)

    def list_indices(self, pattern: str) -> List[str]:
        return self.store.list_indices(pattern)

    def delete_index(self, index: str) -> None:
        self.store.delete_index(index)

    def alias_indices(self, alias: str) -> List[str]:
        return self.store.alias_indices(alias)

    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> bool:
        return self.store.update_alias(alias, add, remove)
