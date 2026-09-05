"""Daily maintenance steps. Plain functions so the Airflow DAG stays a thin wiring layer."""
import glob
import os
import shutil
import tempfile

from pyspark.sql import SparkSession

from signalforge.config import Settings, settings
from signalforge.pipeline.job import run_batch
from signalforge.search.store import SearchStore, index_name


def day_path(archive_dir: str, day: str) -> str:
    return os.path.join(archive_dir, "day=%s" % day)


def compact_day(spark: SparkSession, day: str, cfg: Settings = settings, target_files: int = 1) -> int:
    """Rewrite the many small micro-batch files for one day partition into `target_files` files."""
    src = day_path(cfg.archive_dir, day)
    if not os.path.isdir(src):
        return 0
    before = len(glob.glob(os.path.join(src, "*.parquet")))
    tmp = tempfile.mkdtemp(prefix="compact-", dir=cfg.archive_dir)
    try:
        (spark.read.parquet(src).coalesce(target_files)
         .write.mode("overwrite").parquet(tmp))
        shutil.rmtree(src)
        os.rename(tmp, src)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return before


def reindex_day(spark: SparkSession, day: str, store: SearchStore, cfg: Settings = settings) -> int:
    """Recompute the day's documents from the archive; upsert overwrites whatever streaming wrote."""
    if not os.path.isdir(day_path(cfg.archive_dir, day)):
        return 0
    return run_batch(spark, cfg.archive_dir, store, cfg, day=day)


def verify_day(day: str, store: SearchStore, expected: int, cfg: Settings = settings) -> int:
    idx = index_name(cfg.index_prefix, day)
    store.refresh(idx)
    got = store.count(idx)
    if got < expected:
        raise RuntimeError("%s has %d docs, expected >= %d" % (idx, got, expected))
    return got
