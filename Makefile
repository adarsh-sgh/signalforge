PY := .venv/bin/python
export JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export PYTHONPATH := .
SINK_FLAG := $(if $(SINK),--sink $(SINK))

.PHONY: venv proto test bench up down produce stream batch api airflow

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

proto:
	$(PY) -m grpc_tools.protoc -Iproto --python_out=signalforge/events proto/signal_event.proto

test:
	$(PY) -m pytest -q

bench:
	$(PY) -m bench.bench --rows 1000000 $(SINK_FLAG)

up:
	docker compose up -d && docker compose exec redpanda rpk topic create signals -p 4 || true

down:
	docker compose down -v

produce:
	$(PY) -m signalforge.producer --n 5000

stream:
	$(PY) -m signalforge.pipeline.job --mode stream $(SINK_FLAG)

batch:
	$(PY) -m signalforge.pipeline.job --mode batch --source data/archive $(if $(DAY),--day $(DAY)) $(SINK_FLAG)

api:
	$(if $(SINK),SF_SINK=$(SINK)) $(PY) -m signalforge.api.app

airflow:
	.venv/bin/pip install -q -r requirements-airflow.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.9.txt
	AIRFLOW_HOME=$(PWD)/airflow AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False .venv/bin/airflow standalone
