from pyspark.sql import types as T

from signalforge.events.codec import encode
from signalforge.pipeline import transform
from signalforge.pipeline.job import run_batch
from signalforge.pipeline.sink import write_dataframe
from signalforge.search.store import InMemoryStore
from tests.conftest import D1, D2, EVENTS


def test_batch_dedups_and_aggregates_per_entity_per_day(spark, cfg, archive):
    store = InMemoryStore()
    assert run_batch(spark, archive, store, cfg) == 4  # 2 entities x 2 days
    assert sorted(store.indices) == ["test-%s" % D1, "test-%s" % D2]
    e1 = store.get("test-%s" % D1, "ent-1")
    assert e1["n"] == 3  # redelivered event dropped
    assert e1["mean_score"] == 3.6667 and e1["min_score"] == 2.0 and e1["max_score"] == 5.0
    assert e1["last_score"] == 5.0 and e1["last_ts"] == EVENTS[2]["ts"]
    assert sorted(e1["signal_types"]) == ["rating", "review"] and sorted(e1["sources"]) == ["mobile", "web"]
    assert e1["window_start"].startswith(D1 + "T00:00:00")
    assert store.get("test-%s" % D2, "ent-2")["n"] == 1
    # replaying only one day touches only that index and is idempotent
    assert run_batch(spark, archive, store, cfg, day=D2) == 2
    assert store.count("test-%s" % D1) == 2 and store.get("test-%s" % D1, "ent-1") == e1


def test_kafka_bytes_decode_and_poison_dropped(spark):
    payloads = [(encode(**{k: e[k] for k in ("entity_id", "signal_type", "score", "source", "ts")}),)
                for e in EVENTS[:3]] + [(b"garbage",), (None,)]
    raw = spark.createDataFrame(payloads, T.StructType([T.StructField("value", T.BinaryType())]))
    events = transform.decode_kafka(raw)
    assert events.count() == 3
    assert set(events.columns) == set(transform.EVENT_SCHEMA.names)
    docs = transform.events_to_documents(events).collect()
    assert len(docs) == 1 and docs[0]["n"] == 3 and docs[0]["day"] == D1


def test_streaming_update_mode_upserts_running_aggregates(spark, cfg, archive):
    """File source stands in for Kafka: same dedup/aggregate graph, update mode, availableNow."""
    store = InMemoryStore()
    stream = spark.readStream.schema(transform.EVENT_SCHEMA.add("day", "string")).parquet(archive).drop("day")
    agg = transform.aggregate(transform.dedup(transform.with_event_time(stream), cfg.watermark), cfg.window)
    q = (agg.writeStream.outputMode("update")
         .foreachBatch(lambda b, _: write_dataframe(store, cfg.index_prefix, transform.to_documents(b)))
         .option("checkpointLocation", cfg.checkpoint_dir).trigger(availableNow=True).start())
    q.awaitTermination(120)
    assert store.count("test-%s" % D1) == 2 and store.get("test-%s" % D1, "ent-1")["n"] == 3
