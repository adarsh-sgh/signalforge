"""Spark entrypoint. `--mode stream` tails Kafka; `--mode batch` replays a Parquet directory."""
import argparse
import os
import sys
import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from signalforge.config import Settings, settings
from signalforge.lake.delta import DeltaLake, lake_packages
from signalforge.metrics import BATCH_ROWS, BATCH_SECONDS, EVENTS_DECODED, LAKE_ROWS, serve
from signalforge.pipeline import transform
from signalforge.pipeline.sink import write_dataframe
from signalforge.search.store import SearchStore
from signalforge.sinks import SINKS, open_sink
from signalforge.tenancy import Quota, Router

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(app: str = "signalforge", local: bool = True, kafka: bool = False,
                packages=(), configs=None) -> SparkSession:
    # Workers must run the same interpreter (venv) as the driver, otherwise the protobuf UDF
    # runs under whatever `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # UTC so day windows and index names agree regardless of where the driver runs.
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC"))
    if local:
        b = b.master("local[*]")
    pkgs = ([KAFKA_PKG] if kafka else []) + list(packages)
    if pkgs:
        b = b.config("spark.jars.packages", ",".join(pkgs))
    for key, value in (configs or {}).items():
        b = b.config(key, value)
    return b.getOrCreate()


def build_spark_for(cfg: Settings = settings, app: str = "signalforge", kafka: bool = False) -> SparkSession:
    """Spark session with the delta / s3a jars and configs the lake leg needs, if one is configured."""
    lake = DeltaLake.from_settings(cfg)
    if lake is None:
        return build_spark(app, kafka=kafka)
    return build_spark(app, kafka=kafka, packages=lake_packages(lake.path), configs=lake.configs(cfg))


def read_parquet_events(spark: SparkSession, path: str, day: Optional[str] = None) -> DataFrame:
    df = spark.read.schema(transform.ARCHIVE_SCHEMA).parquet(path)
    if day:
        df = df.where(F.col("day") == day)
    return df.drop("day")


def run_batch(spark: SparkSession, source: str, store: SearchStore, cfg: Settings = settings,
              day: Optional[str] = None, quota: Optional[Quota] = None,
              lake: Optional[DeltaLake] = None) -> int:
    """Replay archived events for one day (or all) and upsert the resulting documents."""
    t0 = time.time()
    events = read_parquet_events(spark, source, day)
    docs = transform.events_to_documents(events, cfg.window)
    if lake is not None:
        docs = docs.persist()  # the lake write and the sink write are two passes over the same plan
        LAKE_ROWS.inc(lake.rewrite_day(docs, day) if day else lake.write(docs, merge_schema=True))
    n = write_dataframe(store, Router.from_settings(cfg), docs, quota or Quota.from_settings(cfg))
    if lake is not None:
        docs.unpersist()
    BATCH_SECONDS.observe(time.time() - t0)
    return n


def run_stream(spark: SparkSession, store: SearchStore, cfg: Settings = settings,
               once: bool = False, lake: Optional[DeltaLake] = None):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
           .option("subscribe", cfg.kafka_topic)
           .option("startingOffsets", "earliest")
           .load())
    events = transform.with_event_time(transform.decode_kafka(raw))
    router, quota = Router.from_settings(cfg), Quota.from_settings(cfg)

    def archive(batch: DataFrame, _id: int) -> None:
        # Raw decoded events go to Parquet partitioned by day; the daily DAG compacts and re-indexes them.
        t0 = time.time()
        batch.persist()
        n = batch.count()
        BATCH_ROWS.set(n)
        EVENTS_DECODED.inc(n)
        batch.write.mode("append").partitionBy("day").parquet(cfg.archive_dir)
        batch.unpersist()
        BATCH_SECONDS.observe(time.time() - t0)

    def upsert(batch: DataFrame, _id: int) -> None:
        docs = transform.to_documents(batch)
        if lake is not None:
            docs = docs.persist()
            LAKE_ROWS.inc(lake.write(docs, merge_schema=True))
        write_dataframe(store, router, docs, quota)
        if lake is not None:
            docs.unpersist()

    agg = transform.aggregate(transform.dedup(events, cfg.watermark), cfg.window)
    trigger = {"availableNow": True} if once else {"processingTime": "10 seconds"}
    q_archive = (events.writeStream.foreachBatch(archive).outputMode("append")
                 .option("checkpointLocation", cfg.checkpoint_dir + "/archive")
                 .trigger(**trigger).start())
    q_index = (agg.writeStream.foreachBatch(upsert).outputMode("update")
               .option("checkpointLocation", cfg.checkpoint_dir + "/index")
               .trigger(**trigger).start())
    return q_archive, q_index


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=["stream", "batch"], default="stream")
    ap.add_argument("--source", default=settings.archive_dir, help="parquet dir for batch mode")
    ap.add_argument("--day", default=None, help="restrict batch mode to one yyyy-MM-dd")
    ap.add_argument("--once", action="store_true", help="stream: drain what is there and exit")
    ap.add_argument("--sink", choices=SINKS, default=settings.sink, help="rollup store (env SF_SINK)")
    ap.add_argument("--lake", default=settings.lake_path,
                    help="delta table path, e.g. s3a://lake/signals_daily (env SF_LAKE_PATH)")
    args = ap.parse_args()

    serve(settings.metrics_port)
    cfg = Settings(lake_path=args.lake) if args.lake != settings.lake_path else settings
    store = open_sink(cfg, args.sink)
    lake = DeltaLake.from_settings(cfg)
    spark = build_spark_for(cfg, kafka=args.mode == "stream")
    if args.mode == "batch":
        print("upserted %d documents" % run_batch(spark, args.source, store, cfg, day=args.day, lake=lake))
        return
    queries = run_stream(spark, store, cfg, once=args.once, lake=lake)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
