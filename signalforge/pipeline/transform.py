"""Pure DataFrame transforms. Same code path for streaming (Kafka) and batch (Parquet)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from signalforge.events.codec import decode

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("entity_id", T.StringType(), False),
    T.StructField("signal_type", T.StringType(), True),
    T.StructField("score", T.DoubleType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("ts", T.LongType(), False),
])

# Archive layout: events plus the `day` partition column.
ARCHIVE_SCHEMA = T.StructType(EVENT_SCHEMA.fields + [T.StructField("day", T.StringType(), True)])

_decode_udf = F.udf(decode, EVENT_SCHEMA)


def decode_kafka(raw: DataFrame) -> DataFrame:
    """Kafka `value` bytes -> event columns. Poison messages decode to null and are dropped."""
    return (raw.select(_decode_udf(F.col("value")).alias("e"))
            .where(F.col("e").isNotNull())
            .select("e.*"))


def with_event_time(events: DataFrame) -> DataFrame:
    return (events
            .withColumn("event_time", F.timestamp_millis(F.col("ts")))
            .withColumn("day", F.date_format("event_time", "yyyy-MM-dd")))


def dedup(events: DataFrame, watermark: str = None) -> DataFrame:
    """Drop redelivered events by event_id. Streaming needs a watermark to bound state."""
    if watermark:
        events = events.withWatermark("event_time", watermark)
    return events.dropDuplicates(["event_id"])


def aggregate(events: DataFrame, window: str = "1 day") -> DataFrame:
    """Per-entity rollup per tumbling window. Single aggregation so it works in update mode."""
    return (events
            .groupBy("entity_id", F.window("event_time", window).alias("w"))
            .agg(F.count("*").alias("n"),
                 F.avg("score").alias("mean_score"),
                 F.min("score").alias("min_score"),
                 F.max("score").alias("max_score"),
                 F.stddev_pop("score").alias("stddev_score"),
                 F.max_by("score", "ts").alias("last_score"),
                 F.max("ts").alias("last_ts"),
                 F.collect_set("signal_type").alias("signal_types"),
                 F.collect_set("source").alias("sources")))


def to_documents(agg: DataFrame) -> DataFrame:
    """Flatten the window struct into the OpenSearch document shape (timestamps as ISO-8601 UTC)."""
    iso = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    return (agg
            .withColumn("window_start", F.date_format("w.start", iso))
            .withColumn("window_end", F.date_format("w.end", iso))
            .withColumn("day", F.date_format("w.start", "yyyy-MM-dd"))
            .withColumn("mean_score", F.round("mean_score", 4))
            .withColumn("stddev_score", F.round(F.coalesce("stddev_score", F.lit(0.0)), 4))
            .drop("w"))


def events_to_documents(events: DataFrame, window: str = "1 day", watermark: str = None) -> DataFrame:
    return to_documents(aggregate(dedup(with_event_time(events), watermark), window))
