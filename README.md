# signalforge

Streaming aggregation of per-entity signal scores (product review signals, sensor readings, ...).
Protobuf events go through Kafka into a PySpark Structured Streaming job that dedups, windows and
rolls them up per entity; the rollups land in OpenSearch (one index per day, one document per entity)
where a small FastAPI service serves point lookups. An Airflow DAG compacts each day's Parquet archive
and rebuilds that day's index from it.

```
producer ──protobuf──> Kafka (redpanda) ──> Spark Structured Streaming ──upsert──> OpenSearch ──> FastAPI
                                              │  decode UDF · dedup(event_id)        signals-YYYY-MM-DD   /entities/{id}
                                              │  window(1 day) · agg per entity      _id = entity_id      /entities/{id}/history
                                              └──append──> Parquet archive (day=…)
                                                              │
                                     Airflow (daily) ─────────┴─> compact ─> re-index (batch mode) ─> verify
```

The same transform code runs in both modes: `--mode stream` reads Kafka, `--mode batch` replays a
Parquet directory. Prometheus metrics are exposed by the Spark driver (`:9108`) and the API (`/metrics`).

## Run

```
make venv          # python3.9 venv + deps (needs Java 17 for pyspark)
make test          # 8 tests, no docker: pyspark local mode, in-memory OpenSearch/Kafka fakes
make bench         # 1M synthetic rows through the batch path

make up            # redpanda + opensearch via docker compose
make produce       # 5000 protobuf events (5% redelivered) -> topic `signals`
make stream        # Kafka -> OpenSearch, keeps running; add --once to drain and exit
make api           # http://localhost:8000/entities/ent-0001  (SF_API_PORT to change)
make batch DAY=2026-09-05   # rebuild one day's index from data/archive
make airflow       # optional: installs airflow, runs `airflow standalone` with dags/
```

Bench on an M-series laptop, `local[*]`: 1M rows -> 188k documents in 3.6 s, ~280k rows/s
(dedup + day window + 9 aggregates, sink to in-memory store).

## Design notes

- Event ids are a hash of `(entity, type, source, ts)`, so a retried publish is the same event
  and `dropDuplicates` with a watermark removes it; no producer-side idempotence needed.
- One aggregation level (entity x tumbling window) keeps the streaming query in `update` mode;
  every micro-batch emits the running rollup for touched windows and upsert-by-`entity_id`
  makes replays and restarts idempotent. Index-per-day maps 1:1 onto the window.
- Streaming also archives decoded events to Parquet partitioned by day. Batch mode over that
  archive is the recovery path; the DAG runs it nightly so the index never depends on the
  streaming job having been healthy all day.
- OpenSearch and Kafka sit behind small protocols (`SearchStore`, `Producer`) with in-memory
  fakes, so tests cover producer -> protobuf -> Spark -> sink -> API without containers.
- Session timezone is pinned to UTC so window boundaries, index names and API `day` params agree.

## Next

- Per-signal-type breakdown inside the document (needs a second aggregation, i.e. append mode).
- `from_protobuf` via the spark-protobuf jar instead of a Python UDF once the decode path
  dominates the profile.
- Alias rollover to retire old daily indices.
