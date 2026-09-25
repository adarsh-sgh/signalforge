#!/usr/bin/env bash
# Brings the stack up and runs every leg end to end. Needs docker and `make venv`.
#
#   ./scripts/smoke.sh            # full run
#   EVENTS=50000 ./scripts/smoke.sh
#
# Each stage prints what it measured; the README numbers come from this script.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-.venv/bin/python}
EVENTS=${EVENTS:-200000}
LAKE=${LAKE:-s3a://lake/signals_daily}
DAY=${DAY:-$(date -u +%F)}
export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)}
export PYTHONPATH=.
export SF_LAKE_PATH=$LAKE
export SF_SINK=clickhouse

say() { printf '\n=== %s ===\n' "$1"; }

say "1/7 stack up"
docker compose up -d --wait redpanda clickhouse redis s3 trino
docker compose up -d s3-init
docker compose up -d flink-jobmanager flink-taskmanager
docker compose exec -T redpanda rpk topic create signals signal_anomalies signal_late -p 4 || true

say "2/7 flink anomaly job"
docker compose exec -T -e SF_ANOMALY_WINDOW_MS=2000 -e SF_ANOMALY_MIN_SAMPLES=5 \
  -e SF_ANOMALY_LATENESS_MS=1000 -e SF_ANOMALY_MIN_STDDEV=0.05 flink-jobmanager \
  flink run -d -py signalforge/flink/anomaly_job.py -pyclientexec python3 | tail -1

say "3/7 anomaly detection over redpanda (spike + straggler)"
$PY scripts/e2e.py anomaly --windows 14 --per-window 6 --window-ms 2000 --rate 40

say "4/7 produce $EVENTS events"
$PY -m signalforge.producer --n "$EVENTS" --entities 2000 --tenants 8

say "5/7 spark: kafka -> clickhouse + delta on s3"
rm -rf data/checkpoints
$PY -m signalforge.pipeline.job --mode stream --once --sink clickhouse --lake "$LAKE"
$PY -m signalforge.pipeline.job --mode batch --source data/archive --day "$DAY" --sink clickhouse --lake "$LAKE"

say "6/7 trino: register the delta table and govern a query workload"
docker compose exec -T trino trino --execute \
  "CREATE SCHEMA IF NOT EXISTS delta.signals WITH (location = 's3://lake/signals')" >/dev/null
docker compose exec -T trino trino --execute \
  "CALL delta.system.register_table(schema_name => 'signals', table_name => 'signals_daily', table_location => 's3://lake/signals_daily')" >/dev/null 2>&1 || true
docker compose exec -T trino trino --execute \
  "SELECT day, count(*) AS rollups FROM delta.signals.signals_daily GROUP BY day ORDER BY day"
$PY scripts/e2e.py guard --day "$DAY"

say "7/7 freshness SLOs and self-healing"
$PY scripts/e2e.py sla --day "$DAY" --sink clickhouse --heal

say "done"
