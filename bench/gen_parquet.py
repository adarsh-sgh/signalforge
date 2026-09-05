"""Write a synthetic events Parquet file with pyarrow (no Spark needed to generate)."""
import argparse
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from signalforge.producer import SIGNAL_TYPES, SOURCES


def generate(path: str, rows: int, entities: int = 50_000, dup_ratio: float = 0.05, seed: int = 0) -> None:
    rng = np.random.default_rng(seed)
    now = int(time.time() * 1000)
    ent = rng.integers(0, entities, rows)
    st = rng.integers(0, len(SIGNAL_TYPES), rows)
    src = rng.integers(0, len(SOURCES), rows)
    ts = now - rng.integers(0, 3 * 86_400_000, rows)
    score = np.round(rng.normal(3.5, 1.0, rows), 3)
    event_id = np.array(["%d-%d" % (i, seed) for i in range(rows)], dtype=object)
    # duplicate a slice of ids to give dedup something to do
    n_dup = int(rows * dup_ratio)
    event_id[:n_dup] = event_id[n_dup:2 * n_dup]
    day = (ts // 86_400_000).astype("datetime64[D]").astype(str)
    table = pa.table({
        "event_id": pa.array(event_id, pa.string()),
        "entity_id": pa.array(["ent-%05d" % e for e in ent], pa.string()),
        "signal_type": pa.array([SIGNAL_TYPES[i] for i in st], pa.string()),
        "score": pa.array(score, pa.float64()),
        "source": pa.array([SOURCES[i] for i in src], pa.string()),
        "ts": pa.array(ts, pa.int64()),
        "day": pa.array(day, pa.string()),
    })
    os.makedirs(path, exist_ok=True)
    pq.write_table(table, os.path.join(path, "events.parquet"), row_group_size=100_000)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="bench/out/events")
    ap.add_argument("--rows", type=int, default=1_000_000)
    a = ap.parse_args()
    generate(a.out, a.rows)
    print("wrote %d rows to %s" % (a.rows, a.out))
