from fastapi.testclient import TestClient

from signalforge.api.app import create_app
from signalforge.clickhouse_store import InMemoryClickHouseStore
from signalforge.config import Settings
from signalforge.metrics import QUOTA_DROPPED
from signalforge.pipeline.job import run_batch
from signalforge.search.store import InMemoryStore
from signalforge.tenancy import Quota, Router
from tests.conftest import D1, D2, ev, write_events

# three tenants on the same day: acme (big, dedicated), beta and gamma (pooled)
EVENTS = [
    ev("ent-1", "review", 4.0, "web", 1_000, tenant="acme"),
    ev("ent-1", "rating", 2.0, "web", 5_000, tenant="acme"),
    ev("ent-2", "review", 5.0, "web", 1_000, tenant="acme"),
    ev("ent-1", "review", 1.0, "web", 1_000, tenant="beta"),   # same entity id as acme's: must not collide
    ev("ent-2", "review", 3.0, "web", 1_000, tenant="beta"),
    ev("ent-3", "review", 3.0, "web", 1_000, tenant="beta"),
    ev("ent-1", "click", 0.5, "web", 1_000, tenant="gamma"),
    ev("ent-1", "click", 0.5, "web", 86_400_000, tenant="gamma"),
]


def test_router_routing_indices_and_parse():
    r = Router("sig", dedicated=frozenset({"acme"}), partitions={"beta": 4})
    assert r.index_for("acme", D1) == "sig-acme-%s" % D1 and r.index_for("beta", D1) == "sig-%s" % D1
    assert r.routing_for("acme", "ent-1") is None and r.routing_for("gamma", "ent-1") == "gamma"
    parts = {r.routing_for("beta", "ent-%d" % i) for i in range(200)}
    assert parts == {"beta#%d" % i for i in range(4)} and r.routing_for("beta", "x") == r.routing_for("beta", "x")
    assert r.alias_for("acme") == "sig-acme" and r.alias_for("beta") == "sig"
    assert r.families() == [("sig", None), ("sig-acme", "acme")]
    assert r.parse_index("sig-%s" % D1) == (None, D1) and r.parse_index("sig-acme-%s" % D1) == ("acme", D1)
    assert r.parse_index("sig-other-%s" % D1) is None and r.parse_index("other-%s" % D1) is None
    cfg = Settings(index_prefix="sig", dedicated_tenants="acme, zed", routing_partitions="beta=4",
                   tenant_quota="beta=10", tenant_quota_default=100)
    assert Router.from_settings(cfg) == Router("sig", frozenset({"acme", "zed"}), {"beta": 4})
    q = Quota.from_settings(cfg)
    assert q.limit_for("beta") == 10 and q.limit_for("acme") == 100
    assert Quota.from_settings(Settings()) is None


def test_batch_routes_tenants_and_enforces_quota(spark, cfg, tmp_path):
    archive = write_events(cfg.archive_dir, EVENTS)
    cfg.dedicated_tenants = "acme"
    cfg.tenant_quota = "beta=2"
    store = InMemoryStore()
    before = QUOTA_DROPPED.labels(tenant="beta")._value.get()
    assert run_batch(spark, archive, store, cfg) == 6  # 7 rollups, one beta entity over quota
    assert sorted(store.indices) == ["test-%s" % D1, "test-%s" % D2, "test-acme-%s" % D1]
    assert store.count("test-acme-%s" % D1) == 2 and store.count("test-%s" % D1) == 3
    assert store.routing["test-acme-%s" % D1]["acme:ent-1"] is None
    assert store.routing["test-%s" % D1]["beta:ent-2"] == "beta"  # pooled tenants pinned to one shard
    assert store.get("test-acme-%s" % D1, "acme:ent-1")["n"] == 2
    assert store.get("test-%s" % D1, "beta:ent-1", "beta")["mean_score"] == 1.0  # no collision on ent-1
    assert store.get("test-%s" % D1, "beta:ent-1") is None  # unrouted GET misses, like OpenSearch would
    assert QUOTA_DROPPED.labels(tenant="beta")._value.get() == before + 1
    # replay is idempotent for admitted entities; the quota still holds
    assert run_batch(spark, archive, store, cfg, day=D1) == 5 and store.count("test-%s" % D1) == 3

    c = TestClient(create_app(store, router=Router.from_settings(cfg)))
    assert c.get("/tenants/acme/entities/ent-1", params={"day": D1}).json()["n"] == 2
    assert c.get("/tenants/beta/entities/ent-1", params={"day": D1}).json()["mean_score"] == 1.0
    assert c.get("/tenants/acme/entities/ent-9", params={"day": D1}).status_code == 404
    assert c.get("/tenants/BAD!/entities/ent-1").status_code == 422
    h = c.get("/tenants/gamma/entities/ent-1/history", params={"end": D2, "days": 7}).json()
    assert h["tenant_id"] == "gamma" and [d["day"] for d in h["daily"]] == [D1, D2] and h["events"] == 2
    assert c.get("/entities/ent-1", params={"day": D1}).status_code == 404  # default tenant has nothing

    # same events into the ClickHouse fake: tenant is part of the key, no routing needed
    ch = InMemoryClickHouseStore()
    assert run_batch(spark, archive, ch, cfg, quota=Quota({})) == 7
    assert ch.get("test-%s" % D1, "beta:ent-3")["n"] == 1 and ch.count("test-%s" % D1) == 6
    c2 = TestClient(create_app(ch, router=Router.from_settings(cfg)))
    assert c2.get("/tenants/beta/entities/ent-3", params={"day": D1}).json()["n"] == 1
