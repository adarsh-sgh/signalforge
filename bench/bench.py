"""Throughput of the batch path: Parquet -> dedup -> aggregate -> documents -> in-memory sink."""
import argparse
import os
import time

from bench.gen_parquet import generate
from signalforge.config import Settings
from signalforge.pipeline.job import build_spark, run_batch
from signalforge.search.store import InMemoryStore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--path", default="bench/out/events")
    a = ap.parse_args()
    if not os.path.exists(os.path.join(a.path, "events.parquet")):
        generate(a.path, a.rows)
    spark = build_spark("sf-bench")
    cfg = Settings(index_prefix="bench", window="1 day")
    store = InMemoryStore()
    run_batch(spark, a.path, store, cfg)  # warm-up: JVM + codegen
    store = InMemoryStore()
    t0 = time.time()
    docs = run_batch(spark, a.path, store, cfg)
    dt = time.time() - t0
    print("rows=%d docs=%d wall=%.2fs throughput=%.0f rows/s" % (a.rows, docs, dt, a.rows / dt))


if __name__ == "__main__":
    main()
