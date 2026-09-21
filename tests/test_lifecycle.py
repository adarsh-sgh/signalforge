import datetime as dt

from signalforge.clickhouse_store import InMemoryClickHouseStore
from signalforge.lifecycle import rollover
from signalforge.search.store import InMemoryStore
from signalforge.tenancy import Router

TODAY = "2026-09-10"


def _day(n):
    return (dt.date.fromisoformat(TODAY) + dt.timedelta(days=n)).isoformat()


def _seed(store, router, days):
    for n in days:
        store.bulk_upsert(router.index_for("beta", _day(n)), [{"tenant_id": "beta", "entity_id": "e", "day": _day(n)}],
                          routing="beta")
        store.bulk_upsert(router.index_for("acme", _day(n)), [{"tenant_id": "acme", "entity_id": "e", "day": _day(n)}])


def test_rollover_precreates_aliases_window_and_retires_old_indices():
    router = Router("sig", dedicated=frozenset({"acme"}))
    store = InMemoryStore()
    _seed(store, router, range(-9, 1))  # ten days of pooled + dedicated indices, plus an unrelated index
    store.ensure_index("sig-other-2026-09-01")
    store.ensure_index("signals-2026-09-01")

    rep = rollover(store, router, TODAY, retention_days=7, shards={"pooled": 3, "acme": 6})
    assert rep.created == ["sig-%s" % _day(1), "sig-acme-%s" % _day(1)]
    assert store.shards["sig-%s" % _day(1)] == 3 and store.shards["sig-acme-%s" % _day(1)] == 6
    kept = [_day(n) for n in range(-6, 2)]  # 7 days ending today, plus tomorrow
    assert store.alias_indices("sig") == ["sig-%s" % d for d in kept]
    assert store.alias_indices("sig-acme") == ["sig-acme-%s" % d for d in kept]
    assert rep.deleted == ["sig-%s" % _day(n) for n in (-9, -8, -7)] + ["sig-acme-%s" % _day(n) for n in (-9, -8, -7)]
    assert "sig-%s" % _day(-7) not in store.indices and "sig-other-2026-09-01" in store.indices
    assert "signals-2026-09-01" in store.indices  # different prefix, untouched
    assert store.get("sig-%s" % _day(-6), "beta:e", "beta")["day"] == _day(-6)

    again = rollover(store, router, TODAY, retention_days=7)
    assert again.created == [] and again.deleted == [] and again.alias_added == {}  # idempotent

    nxt = rollover(store, router, _day(1), retention_days=7)  # next day: one more created, one more retired per family
    assert nxt.created == ["sig-%s" % _day(2), "sig-acme-%s" % _day(2)]
    assert nxt.deleted == ["sig-%s" % _day(-6), "sig-acme-%s" % _day(-6)]
    assert nxt.alias_added["sig"] == ["sig-%s" % _day(2)] and nxt.alias_removed["sig"] == ["sig-%s" % _day(-6)]


def test_rollover_on_clickhouse_drops_partitions():
    router = Router("sig")
    store = InMemoryClickHouseStore()
    _seed(store, router, range(-9, 1))
    rep = rollover(store, router, TODAY, retention_days=3)
    assert rep.created == ["sig-%s" % _day(1)] and rep.alias_added == {}  # no aliases on a single table
    assert rep.deleted == ["sig-%s" % _day(n) for n in range(-9, -2)]
    assert sorted({r[0] for r in store.rows}) == [_day(n) for n in (-2, -1, 0)]
