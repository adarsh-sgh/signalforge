"""Spark entrypoint. `--mode stream` tails Kafka; `--mode batch` replays a Parquet directory."""
import argparse
import os
import sys
import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from signalforge.config import Settings, settings
from signalforge.metrics import BATCH_ROWS, BATCH_SECONDS, EVENTS_DECODED, serve
from signalforge.pipeline import transform
from signalforge.pipeline.sink import write_dataframe
from signalforge.search.store import SearchStore
from signalforge.sinks import SINKS, open_sink

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(app: str = "signalforge", local: bool = True, kafka: bool = False) -> SparkSession:
    # Workers must run the same interpreter (venv) as the driver, otherwise the protobuf UDF
    # runs under whatever `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # UTC so day windows and index names agree regardless of where the driver runs.
    b = (SparkSession.builder.appName(app)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC"))
    if local:
        b = b.master("local[*]")
    if kafka:
        b = b.config("spark.jars.packages", KAFKA_PKG)
    return b.getOrCreate()


def read_parquet_events(spark: SparkSession, path: str, day: Optional[str] = None) -> DataFrame:
    df = spark.read.schema(transform.ARCHIVE_SCHEMA).parquet(path)
    if day:
        df = df.where(F.col("day") == day)
    return df.drop("day")


def run_batch(spark: SparkSession, source: str, store: SearchStore, cfg: Settings = settings,
              day: Optional[str] = None) -> int:
    """Replay archived events for one day (or all) and upsert the resulting documents."""
    t0 = time.time()
    events = read_parquet_events(spark, source, day)
    docs = transform.events_to_documents(events, cfg.window)
    n = write_dataframe(store, cfg.index_prefix, docs)
    BATCH_SECONDS.observe(time.time() - t0)
    return n


def run_stream(spark: SparkSession, store: SearchStore, cfg: Settings = settings,
               once: bool = False):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
           .option("subscribe", cfg.kafka_topic)
           .option("startingOffsets", "earliest")
           .load())
    events = transform.with_event_time(transform.decode_kafka(raw))

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
        write_dataframe(store, cfg.index_prefix, transform.to_documents(batch))

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
    args = ap.parse_args()

    serve(settings.metrics_port)
    store = open_sink(settings, args.sink)
    spark = build_spark(kafka=args.mode == "stream")
    if args.mode == "batch":
        print("upserted %d documents" % run_batch(spark, args.source, store, day=args.day))
        return
    queries = run_stream(spark, store, once=args.once)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
