"""End-to-end checks against the docker-compose stack, and the source of the numbers in the README.

Each subcommand exercises one leg and prints what it measured:

  anomaly   produce a paced event stream with a spike and a straggler into Redpanda, then read the
            Flink job's `signal_anomalies` and `signal_late` topics back
  guard     run a mixed query workload through the admission guard against real Trino, and report
            estimated bytes refused vs bytes actually scanned by the admitted queries
  sla       probe the archive, the lake and the serving store and run one self-healing sweep

`make smoke` runs them in order after `make up`. Everything here talks to the real services; there
are no fakes in this file.
"""
import argparse
import json
import sys
import time

from signalforge.config import Settings, settings
from signalforge.events.codec import encode

QUIET_SCORE = 3.5
SPIKE_SCORE = 12.0


# -- anomaly ------------------------------------------------------------------

def produce_pattern(cfg: Settings, windows: int, per_window: int, window_ms: int,
                    entity: str = "", tenant: str = "t-e2e", rate: float = 60.0) -> dict:
    """`windows` quiet windows, one spiking window, then one event stamped far in the past.

    Event time advances one window per `per_window` messages while wall-clock time advances too, so
    Flink's periodic watermark generator actually runs and the straggler is genuinely late rather
    than merely out of order.
    """
    from signalforge.producer import kafka_producer

    # a fresh entity per run: the Flink job keeps keyed detector state, so re-running against the
    # same entity would be judged against the previous run's baseline
    entity = entity or "ent-e2e-%d" % (time.time() % 100000)
    producer = kafka_producer(cfg.kafka_bootstrap)
    # aligned to a window boundary, so each produced window maps onto exactly one Flink window
    # instead of straddling two and mixing quiet events into the spike
    t0 = ((int(time.time() * 1000) - (windows + 2) * window_ms) // window_ms) * window_ms
    sent = 0
    for w in range(windows + 1):
        score = SPIKE_SCORE if w == windows else QUIET_SCORE + 0.1 * (w % 2)
        for i in range(per_window):
            ts = t0 + w * window_ms + i * (window_ms // max(1, per_window))
            producer.produce(cfg.kafka_topic, key=tenant.encode(),
                             value=encode(entity_id=entity, signal_type="review", score=score,
                                          source="web", ts=ts, tenant_id=tenant))
            sent += 1
            time.sleep(1.0 / rate)
    producer.flush()
    # far behind the newest event time, so its window closed long ago -> late side output
    straggler_ts = t0 + window_ms // 2
    producer.produce(cfg.kafka_topic, key=tenant.encode(),
                     value=encode(entity_id=entity, signal_type="review", score=99.0,
                                  source="web", ts=straggler_ts, tenant_id=tenant))
    # A tumbling window only fires once the watermark passes its end, and the watermark is derived
    # from the newest event time. Without these the spike window would sit open until some later
    # event happened to arrive. They use a different entity, so they are a different key and cannot
    # touch the baseline being measured.
    for k in range(4):
        producer.produce(cfg.kafka_topic, key=tenant.encode(),
                         value=encode(entity_id=entity + "-flush", signal_type="review", score=1.0,
                                      source="web", ts=t0 + (windows + 2 + k) * window_ms,
                                      tenant_id=tenant))
        time.sleep(0.2)
    producer.flush()
    return {"sent": sent + 5, "spike_window_start": t0 + windows * window_ms,
            "straggler_ts": straggler_ts, "entity": entity, "tenant": tenant}


def drain(cfg: Settings, topic: str, seconds: float) -> list:
    from confluent_kafka import Consumer

    consumer = Consumer({"bootstrap.servers": cfg.kafka_bootstrap,
                         "group.id": "e2e-%s-%d" % (topic, time.time()),
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    out, deadline = [], time.time() + seconds
    try:
        while time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            out.append(msg.value().decode())
    finally:
        consumer.close()
    return out


def cmd_anomaly(args) -> int:
    cfg = Settings()
    pattern = produce_pattern(cfg, args.windows, args.per_window, args.window_ms, rate=args.rate)
    print("produced %d events (%d quiet windows of %d, one spike, one straggler)"
          % (pattern["sent"], args.windows, args.per_window))
    anomalies = [json.loads(m) for m in drain(cfg, cfg.anomaly_topic, args.wait)]
    late = [json.loads(m) for m in drain(cfg, cfg.anomaly_late_topic, args.late_wait)]
    mine = [a for a in anomalies if a["entity_id"] == pattern["entity"]]
    print("anomalies on %s: %d (%d for %s)" % (cfg.anomaly_topic, len(anomalies), len(mine),
                                               pattern["entity"]))
    for a in mine[:3]:
        print("  window %d value=%.2f baseline=%.2f z=%.1f detector=%s"
              % (a["window_start"], a["value"], a["baseline"], a["score"], a["detector"]))
    print("late events on %s: %d%s" % (cfg.anomaly_late_topic, len(late),
                                       " (straggler routed)" if any(
                                           e["ts"] == pattern["straggler_ts"] for e in late) else ""))
    if not mine:
        print("FAIL no anomaly for the spike window", file=sys.stderr)
        return 1
    return 0


# -- guard --------------------------------------------------------------------

WORKLOAD = [
    ("analyst", "SELECT entity_id, mean_score FROM {t} WHERE day = '{d}' ORDER BY mean_score DESC LIMIT 5"),
    ("analyst", "SELECT count(*) FROM {t} WHERE day >= '{d}'"),
    ("analyst", "SELECT tenant_id, avg(mean_score) FROM {t} WHERE day IN ('{d}') GROUP BY tenant_id"),
    ("scripted", "SELECT entity_id FROM {t}"),                                  # no partition filter
    ("scripted", "SELECT * FROM {t} WHERE day = '{d}'"),                         # select * no limit
    ("scripted", "SELECT a.entity_id FROM {t} a, {t} b WHERE a.day = '{d}'"),    # cross join
    ("scripted", "DELETE FROM {t} WHERE day = '{d}'"),                           # not a read
    ("scripted", "SELECT entity_id FROM {t} WHERE substr(day, 1, 7) = '2026-09'"),  # unprunable
]


def cmd_guard(args) -> int:
    from signalforge.trino.audit import InMemoryAudit
    from signalforge.trino.client import TrinoClient
    from signalforge.trino.guard import ALLOW, Guard, Policy, human, summarize

    cfg = Settings(trino_max_scan_bytes=args.max_scan_bytes)
    client = TrinoClient(cfg)
    if not client.health():
        print("FAIL trino is not reachable at %s" % cfg.trino_url, file=sys.stderr)
        return 1
    table = "%s.%s.%s" % (cfg.trino_catalog, cfg.trino_schema, cfg.lake_table)
    audit = InMemoryAudit()
    guard = Guard(Policy(tables=Policy.default(cfg.lake_table).tables,
                         max_scan_bytes=cfg.trino_max_scan_bytes, max_concurrent_per_user=2),
                  estimator=client.estimate_scan_bytes, audit=audit)

    scanned = 0
    for user, template in WORKLOAD:
        sql = template.format(t=table, d=args.day)
        decision = guard.admit(user, sql)
        note = ""
        if decision.verdict == ALLOW:
            try:
                result = client.run(sql, user=user)
                scanned += result.processed_bytes or 0
                note = " -> %d rows, %s scanned" % (len(result.rows), human(result.processed_bytes))
            finally:
                guard.release(user)
        print("%-9s %-8s %-30s %s%s" % (user, decision.verdict, decision.rule,
                                        human(decision.estimated_bytes), note))
        if decision.verdict != ALLOW:
            print("          %s" % decision.reason)

    report = summarize(audit.decisions)
    print("\n%d queries: %s" % (report["total"], report["by_verdict"]))
    print("estimated bytes refused by the guard: %s" % human(report["estimated_bytes_blocked"]))
    print("bytes Trino actually processed for admitted queries: %s" % human(scanned))
    return 0


# -- sla ----------------------------------------------------------------------

def cmd_sla(args) -> int:
    from signalforge import backfill, sla
    from signalforge.lake.delta import DeltaLake
    from signalforge.pipeline.job import build_spark_for
    from signalforge.sinks import open_sink
    from signalforge.tenancy import Router

    cfg = Settings()
    lake = DeltaLake.from_settings(cfg)
    spark = build_spark_for(cfg, "sf-e2e-sla")
    store = open_sink(cfg, args.sink)
    now_ms = int(time.time() * 1000)
    readings = [sla.archive_freshness(cfg.archive_dir, args.day),
                sla.store_freshness(store, Router.from_settings(cfg).day_indices(args.day), args.day,
                                    updated_at_ms=now_ms)]
    if lake is not None:
        readings.append(sla.lake_freshness(lake, spark, args.day))
    for r in readings:
        print("%-8s updated_at=%s rows=%d lag=%ss %s"
              % (r.dataset, r.updated_at_ms, r.rows, r.lag_seconds(now_ms), r.detail))
    slos = sla.slos_from_settings(cfg)

    def rebuild(breach):
        """The real repair: recompute the day from the Parquet archive into both legs."""
        return backfill.reindex_day(spark, breach.day, store, cfg, lake=lake)

    healer = sla.Healer(slos, {sla.BACKFILL: rebuild if args.heal else (lambda b: 0)})
    print(json.dumps(sla.sweep(readings, slos, healer, now_ms=now_ms), indent=2, default=str))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("anomaly", help="flink anomaly detection over redpanda")
    a.add_argument("--windows", type=int, default=12)
    a.add_argument("--per-window", type=int, default=6)
    a.add_argument("--window-ms", type=int, default=2000)
    a.add_argument("--rate", type=float, default=60.0, help="events/sec, paces event time vs wall clock")
    a.add_argument("--wait", type=float, default=30.0, help="seconds to read the anomaly topic")
    a.add_argument("--late-wait", type=float, default=15.0, help="seconds to read the late topic")
    a.set_defaults(func=cmd_anomaly)

    g = sub.add_parser("guard", help="query governance against real trino")
    g.add_argument("--day", default=time.strftime("%Y-%m-%d"))
    g.add_argument("--max-scan-bytes", type=int, default=2 << 20,
                   help="byte budget the scan-estimate rule enforces")
    g.set_defaults(func=cmd_guard)

    s = sub.add_parser("sla", help="freshness probes and one self-healing sweep")
    s.add_argument("--day", default=time.strftime("%Y-%m-%d"))
    s.add_argument("--sink", default=settings.sink)
    s.add_argument("--heal", action="store_true", help="actually re-run the day's backfill on a breach")
    s.set_defaults(func=cmd_sla)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
