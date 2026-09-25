"""Flink anomaly detection over the same protobuf event stream the Spark job consumes.

    Redpanda `signals` --> decode --> watermarks --> tumbling event-time window per
                                          |          (tenant, entity): n/sum/min/max
                                          |                    |
                                     late side output           v
                                          |          keyed detector state (z-score | EWMA)
                                          v                    |
                              Redpanda `signal_late`            v
                                                     Redpanda `signal_anomalies` (JSON)

The window operator runs with `allowed_lateness = 0` on purpose: a straggler past the
watermark would otherwise re-fire an already-closed window *after* later windows had
updated the detector baseline, so the baseline would see windows out of order. Late
events go to their own topic instead and are picked up by the nightly Spark replay of
the Parquet archive, which recomputes the day from scratch.

`anomaly_pipeline` takes and returns plain DataStreams so the whole graph runs on a
local MiniCluster in tests with `from_collection` instead of Kafka.
"""
import argparse
import os
from typing import Iterable, Optional, Tuple

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import ByteArraySchema, SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (AggregateFunction, KeyedProcessFunction, MapFunction, OutputTag,
                                ProcessWindowFunction, RuntimeContext, RuntimeExecutionMode,
                                StreamExecutionEnvironment)
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
                                                 KafkaSink, KafkaSource)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from signalforge.anomaly import DetectorConfig, DetectorState, WindowStat, update
from signalforge.config import Settings, settings
from signalforge.events.codec import decode

def event_type():
    """A fresh TypeInformation each call: once one has been handed to the JVM it holds a Java
    object and cloudpickle can no longer ship it inside an operator (the late-data OutputTag)."""
    return Types.ROW_NAMED(["tenant_id", "entity_id", "score", "ts"],
                           [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])


EVENT_TYPE = event_type()
ACC_TYPE = Types.TUPLE([Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])
STAT_TYPE = Types.ROW_NAMED(
    ["tenant_id", "entity_id", "window_start", "window_end", "n", "sum_score", "min_score", "max_score"],
    [Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])
LATE_TAG_NAME = "late-events"


def event_key(row) -> str:
    return "%s:%s" % (row[0], row[1])


class _EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[3]


class _DecodeProtobuf(MapFunction):
    """Kafka value bytes -> event row. Undecodable payloads become None and are filtered out."""

    def map(self, raw: bytes):
        e = decode(raw)
        if e is None:
            return None
        from pyflink.common import Row

        return Row(tenant_id=e["tenant_id"], entity_id=e["entity_id"], score=e["score"], ts=e["ts"])


class WindowAgg(AggregateFunction):
    """n / sum / min / max per (key, window). Same nine-ish aggregates as the Spark rollup, trimmed
    to what the detector needs so the window state stays four numbers per key."""

    def create_accumulator(self) -> Tuple[int, float, float, float]:
        return 0, 0.0, float("inf"), float("-inf")

    def add(self, value, acc):
        n, total, lo, hi = acc
        s = value[2]
        return n + 1, total + s, min(lo, s), max(hi, s)

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return a[0] + b[0], a[1] + b[1], min(a[2], b[2]), max(a[3], b[3])


class AttachWindow(ProcessWindowFunction):
    """Stamp the aggregate with its window bounds and the key it came from."""

    def process(self, key: str, context: "ProcessWindowFunction.Context", elements: Iterable):
        from pyflink.common import Row

        tenant, _, entity = key.partition(":")
        n, total, lo, hi = next(iter(elements))
        yield Row(tenant_id=tenant, entity_id=entity, window_start=context.window().start,
                  window_end=context.window().end, n=n, sum_score=total, min_score=lo, max_score=hi)


class Detect(KeyedProcessFunction):
    """The statistical detector, with its baseline in Flink keyed state (JSON, one row per key)."""

    def __init__(self, cfg: DetectorConfig) -> None:
        self.cfg = cfg
        self.state = None
        self.windows = self.anomalies = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self.state = runtime_context.get_state(ValueStateDescriptor("detector", Types.STRING()))
        group = runtime_context.get_metrics_group()
        self.windows = group.counter("sf_windows_scored")
        self.anomalies = group.counter("sf_anomalies_emitted")

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        raw = self.state.value()
        before = DetectorState.from_json(raw) if raw else DetectorState()
        stat = WindowStat(value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7])
        after, hit = update(before, stat, self.cfg)
        self.state.update(after.to_json())
        self.windows.inc()
        if hit is not None:
            self.anomalies.inc()
            yield hit.to_json()


class CountLate(MapFunction):
    """Late events are forwarded verbatim; the counter is what the SLA check reads.

    A MapFunction rather than a plain callable, because only a real function class gets `open`
    called and the counter would otherwise still be None on the first record.
    """

    def __init__(self) -> None:
        self.late = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self.late = runtime_context.get_metrics_group().counter("sf_late_events")

    def map(self, value):
        self.late.inc()
        return value


def decode_events(raw):
    """Kafka bytes stream -> typed event stream."""
    return raw.map(_DecodeProtobuf(), output_type=EVENT_TYPE).filter(lambda r: r is not None)


def anomaly_pipeline(events, cfg: Settings = settings, detector: Optional[DetectorConfig] = None):
    """events: DataStream[EVENT_TYPE] -> (window stats, anomaly JSON, late events)."""
    detector = detector or DetectorConfig.from_settings(cfg)
    late_tag = OutputTag(LATE_TAG_NAME, event_type())
    watermarks = (WatermarkStrategy
                  .for_bounded_out_of_orderness(Duration.of_millis(cfg.anomaly_lateness_ms))
                  .with_timestamp_assigner(_EventTime()))
    stats = (events
             .assign_timestamps_and_watermarks(watermarks)
             .key_by(event_key, key_type=Types.STRING())
             .window(TumblingEventTimeWindows.of(Time.milliseconds(cfg.anomaly_window_ms)))
             .allowed_lateness(0)
             .side_output_late_data(late_tag)
             .aggregate(WindowAgg(), window_function=AttachWindow(),
                        accumulator_type=ACC_TYPE, output_type=STAT_TYPE))
    anomalies = (stats.key_by(event_key, key_type=Types.STRING())
                 .process(Detect(detector), output_type=Types.STRING()))
    return stats, anomalies, stats.get_side_output(late_tag)


def build_env(parallelism: int = 1, jars: str = "") -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    # AUTOMATIC would run a bounded source in BATCH mode, where watermarks never advance mid-stream
    # and nothing is ever late; the job is a streaming job either way.
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(parallelism)
    for jar in filter(None, (j.strip() for j in jars.split(";"))):
        env.add_jars(jar if jar.startswith("file:") else "file://" + os.path.abspath(jar))
    return env


def kafka_source(cfg: Settings, group: str = "signalforge-flink") -> KafkaSource:
    return (KafkaSource.builder()
            .set_bootstrap_servers(cfg.kafka_bootstrap)
            .set_topics(cfg.kafka_topic)
            .set_group_id(group)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(ByteArraySchema())
            .build())


def kafka_sink(cfg: Settings, topic: str) -> KafkaSink:
    return (KafkaSink.builder()
            .set_bootstrap_servers(cfg.kafka_bootstrap)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .build())


def main() -> None:
    ap = argparse.ArgumentParser(description="Flink anomaly detection over the signals topic")
    ap.add_argument("--parallelism", type=int, default=int(os.environ.get("SF_FLINK_PARALLELISM", "1")))
    ap.add_argument("--jars", default=os.environ.get("SF_FLINK_JARS", ""))
    ap.add_argument("--threshold", type=float, default=settings.anomaly_threshold)
    ap.add_argument("--mode", default=settings.anomaly_mode, help="zscore | ewma")
    ap.add_argument("--window-ms", type=int, default=settings.anomaly_window_ms)
    ap.add_argument("--print", action="store_true", help="print anomalies instead of writing Kafka")
    a = ap.parse_args()

    cfg = Settings(anomaly_mode=a.mode, anomaly_threshold=a.threshold, anomaly_window_ms=a.window_ms)
    env = build_env(a.parallelism, a.jars)
    raw = env.from_source(kafka_source(cfg), WatermarkStrategy.no_watermarks(), "signals")
    _, anomalies, late_raw = anomaly_pipeline(decode_events(raw), cfg)
    late = late_raw.map(CountLate(), output_type=event_type())
    if a.print:
        anomalies.print()
        late.map(lambda r: "late %s" % (r,), output_type=Types.STRING()).print()
    else:
        anomalies.sink_to(kafka_sink(cfg, cfg.anomaly_topic))
        (late.map(lambda r: '{"tenant_id":"%s","entity_id":"%s","ts":%d}' % (r[0], r[1], r[3]),
                  output_type=Types.STRING())
         .sink_to(kafka_sink(cfg, cfg.anomaly_late_topic)))
    env.execute("signalforge-anomalies")


if __name__ == "__main__":
    main()
