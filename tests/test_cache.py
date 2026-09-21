import json

from fastapi.testclient import TestClient

from signalforge.api.app import create_app
from signalforge.cache import CachedStore, InMemoryCache, RedisCache, cache_key
from signalforge.pipeline.job import run_batch
from signalforge.search.store import InMemoryStore
from tests.conftest import D1


def test_lookup_hits_cache_until_upsert_or_ttl(spark, cfg, archive):
    now = [0.0]
    inner, cache = InMemoryStore(), InMemoryCache(clock=lambda: now[0])
    store = CachedStore(inner, cache, ttl=60)
    run_batch(spark, archive, store, cfg)
    c = TestClient(create_app(store, prefix="test"))
    idx, key = "test-%s" % D1, cache_key("default:ent-1", D1)

    assert c.get("/entities/ent-1", params={"day": D1}).json()["n"] == 3  # miss -> filled
    assert cache.data[key][1]["n"] == 3
    inner.indices[idx]["default:ent-1"]["n"] = 99  # change behind the cache: still served from cache
    assert c.get("/entities/ent-1", params={"day": D1}).json()["n"] == 3
    assert c.get("/entities/ent-9", params={"day": D1}).status_code == 404 and cache_key("default:ent-9", D1) not in cache.data

    store.bulk_upsert(idx, [dict(inner.indices[idx]["default:ent-1"], n=7)], routing="default")  # sink write evicts that entity-day
    assert key not in cache.data
    assert c.get("/entities/ent-1", params={"day": D1}).json()["n"] == 7
    now[0] = 61  # TTL elapsed
    inner.indices[idx]["default:ent-1"]["n"] = 8
    assert c.get("/entities/ent-1", params={"day": D1}).json()["n"] == 8
    assert c.get("/entities/ent-1/history", params={"end": D1, "days": 1}).json()["events"] == 8  # history bypasses cache

    m = c.get("/metrics").text
    assert 'sf_cache_lookups_total{result="hit"} 1.0' in m and 'sf_cache_lookups_total{result="miss"} 4.0' in m


class _Redis:
    def __init__(self):
        self.kv, self.ex = {}, {}

    def get(self, k):
        return self.kv.get(k)

    def set(self, k, v, ex=None):
        self.kv[k], self.ex[k] = v, ex

    def delete(self, *keys):
        for k in keys:
            self.kv.pop(k, None)


def test_redis_cache_json_with_ttl():
    r = _Redis()
    cache = RedisCache("redis://unused", client=r)
    doc = {"entity_id": "ent-1", "day": D1, "n": 3, "signal_types": ["review"]}
    assert cache.get("entity:ent-1:%s" % D1) is None
    cache.set("entity:ent-1:%s" % D1, doc, ttl=60)
    assert r.ex["entity:ent-1:%s" % D1] == 60 and json.loads(r.kv["entity:ent-1:%s" % D1]) == doc
    assert cache.get("entity:ent-1:%s" % D1) == doc
    cache.delete(["entity:ent-1:%s" % D1, "entity:missing:%s" % D1])
    assert r.kv == {}
