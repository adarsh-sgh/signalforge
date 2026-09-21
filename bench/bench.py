"""Throughput of the batch path: Parquet -> dedup -> aggregate -> documents -> in-memory sink fake.
With --dedicated / --quota the run also reports how the multi-tenant routing fanned the documents out."""
import argparse
import os
import time

from bench.gen_parquet import generate
from signalforge.clickhouse_store import InMemoryClickHouseStore
from signalforge.config import Settings
from signalforge.pipeline.job import build_spark, run_batch
from signalforge.search.store import InMemoryStore
from signalforge.tenancy import Quota

FAKES = {"opensearch": InMemoryStore, "clickhouse": InMemoryClickHouseStore}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--path", default="bench/out/events")
    ap.add_argument("--sink", choices=sorted(FAKES), default="opensearch")
    ap.add_argument("--tenants", type=int, default=200, help="distinct tenants in the generated data")
    ap.add_argument("--dedicated", default="", help="comma list of tenants with their own daily index, e.g. t-000")
    ap.add_argument("--quota", type=int, default=0, help="max entities per tenant per day, 0 = unlimited")
    a = ap.parse_args()
    if not os.path.exists(os.path.join(a.path, "events.parquet")):
        generate(a.path, a.rows, tenants=a.tenants)
    spark = build_spark("sf-bench")
    cfg = Settings(index_prefix="bench", window="1 day", dedicated_tenants=a.dedicated, tenant_quota_default=a.quota)
    run_batch(spark, a.path, FAKES[a.sink](), cfg, quota=Quota({}, a.quota))  # warm-up: JVM + codegen
    store = FAKES[a.sink]()
    quota = Quota({}, a.quota)
    t0 = time.time()
    docs = run_batch(spark, a.path, store, cfg, quota=quota)
    dt = time.time() - t0
    print("sink=%s rows=%d docs=%d wall=%.2fs throughput=%.0f rows/s" % (a.sink, a.rows, docs, dt, a.rows / dt))
    if a.sink == "opensearch":
        for idx in sorted(store.indices):
            routes = {r for r in store.routing[idx].values() if r}
            print("  %-24s docs=%-7d routing keys=%d" % (idx, store.count(idx), len(routes)))
    if a.quota:
        dropped = sum(max(0, len(s) - a.quota) for s in quota.admitted.values())
        over = sum(1 for s in quota.admitted.values() if len(s) >= a.quota)
        print("  quota=%d entities/tenant/day: %d tenant-days at the cap" % (a.quota, over))


if __name__ == "__main__":
    main()
