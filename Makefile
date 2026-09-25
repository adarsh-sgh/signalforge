PY := .venv/bin/python
export JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export PYTHONPATH := .
SINK_FLAG := $(if $(SINK),--sink $(SINK))

.PHONY: venv proto test bench up down produce stream batch api airflow flink-image flink-test flink-job guard trino-sql lake-register

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

proto:
	$(PY) -m grpc_tools.protoc -Iproto --python_out=signalforge/events proto/signal_event.proto

test:
	$(PY) -m pytest -q

bench:
	$(PY) -m bench.bench --rows 1000000 $(SINK_FLAG) $(BENCH_ARGS)

up:
	docker compose up -d
	docker compose exec -T redpanda rpk topic create signals signal_anomalies signal_late -p 4 || true

down:
	docker compose down -v

produce:
	$(PY) -m signalforge.producer --n 5000

stream:
	$(PY) -m signalforge.pipeline.job --mode stream $(SINK_FLAG)

batch:
	$(PY) -m signalforge.pipeline.job --mode batch --source data/archive $(if $(DAY),--day $(DAY)) $(SINK_FLAG)

# same job, plus the Delta leg on the S3 store: SF_LAKE_PATH=s3a://lake/signals_daily
lake-batch:
	SF_LAKE_PATH=$(or $(LAKE),s3a://lake/signals_daily) \
	  $(PY) -m signalforge.pipeline.job --mode batch --source data/archive $(if $(DAY),--day $(DAY)) $(SINK_FLAG)

api:
	$(if $(SINK),SF_SINK=$(SINK)) $(PY) -m signalforge.api.app

airflow:
	.venv/bin/pip install -q -r requirements-airflow.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.9.txt
	AIRFLOW_HOME=$(PWD)/airflow AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False .venv/bin/airflow standalone

flink-image:
	docker build --platform linux/amd64 -f docker/flink.Dockerfile -t signalforge-flink:dev .

# PyFlink is x86_64-only on Linux, so the MiniCluster tests run in the image (emulated on arm64).
flink-test: flink-image
	docker run --rm --platform linux/amd64 -v "$(PWD)":/opt/signalforge signalforge-flink:dev \
	  python -m pytest -q tests/flink

# submit the anomaly job to the compose cluster; FLINK_ARGS="--mode ewma --threshold 2.5"
flink-job:
	docker compose exec -T flink-jobmanager flink run -py signalforge/flink/anomaly_job.py \
	  -pyclientexec python3 --jarfile /opt/flink/lib/flink-sql-connector-kafka.jar $(FLINK_ARGS)

# admission service in front of trino (SF_TRINO_GUARD_PORT, SF_TRINO_AUDIT_FILE)
guard:
	$(PY) -m signalforge.trino.service

trino-sql:
	docker compose exec -T trino trino --execute "$(SQL)"

# give the Delta table Spark wrote a name Trino can query (file metastore, so it must be registered)
lake-register:
	docker compose exec -T trino trino --execute \
	  "CREATE SCHEMA IF NOT EXISTS delta.signals WITH (location = 's3://lake/signals')"
	docker compose exec -T trino trino --execute \
	  "CALL delta.system.register_table(schema_name => 'signals', table_name => 'signals_daily', table_location => 's3://lake/signals_daily')"
