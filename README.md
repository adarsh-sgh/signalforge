# signalforge

Streaming aggregation of per-entity signal scores (product review signals, sensor readings, ...).
Protobuf events go through Kafka into a PySpark Structured Streaming job that dedups, windows and
rolls them up per entity; the rollups land in OpenSearch (one index per day, one document per entity)
or, with `--sink clickhouse`, in a single ClickHouse `ReplacingMergeTree` table. A small FastAPI service
serves point lookups from whichever sink is configured, optionally through a Redis read-through cache.
An Airflow DAG compacts each day's Parquet archive and rebuilds that day's rollups from it.

```
producer ──protobuf──> Kafka (redpanda) ──> Spark Structured Streaming ──upsert──> OpenSearch ──────> FastAPI
                                              │  decode UDF · dedup(event_id)   │     signals-YYYY-MM-DD      │  /entities/{id}
                                              │  window(1 day) · agg per entity │     _id = entity_id         │  /entities/{id}/history
                                              │                                 └──> ClickHouse (SF_SINK)     │
                                              │                                      signals_daily            │
                                              │                                      ReplacingMergeTree       │
                                              │                                      (day, entity_id)  ──evict──> Redis (REDIS_URL)
                                              └──append──> Parquet archive (day=…)                              entity:{id}:{day}, TTL
                                                              │
                                     Airflow (daily) ─────────┴─> compact ─> re-index (batch mode) ─> verify
```

The same transform code runs in both modes: `--mode stream` reads Kafka, `--mode batch` replays a
Parquet directory. Prometheus metrics are exposed by the Spark driver (`:9108`) and the API (`/metrics`).

## Run

```
make venv          # python3.9 venv + deps (needs Java 17 for pyspark)
make test          # 12 tests, no docker: pyspark local mode, in-memory OpenSearch/ClickHouse/Redis/Kafka fakes
make bench         # 1M synthetic rows through the batch path; SINK=clickhouse for the ClickHouse fake

make up            # redpanda + opensearch + clickhouse + redis via docker compose
make produce       # 5000 protobuf events (5% redelivered) -> topic `signals`
make stream        # Kafka -> OpenSearch, keeps running; add --once to drain and exit
make api           # http://localhost:8000/entities/ent-0001  (SF_API_PORT to change)
make batch DAY=2026-09-05   # rebuild one day's index from data/archive
make airflow       # optional: installs airflow, runs `airflow standalone` with dags/

make stream SINK=clickhouse             # same job, rollups into ClickHouse `signals_daily`
make batch  SINK=clickhouse DAY=2026-09-05
make api    SINK=clickhouse             # or SF_SINK=clickhouse; SF_CLICKHOUSE_URL=http://localhost:8123
REDIS_URL=redis://localhost:6379 make api   # point lookups cached, SF_CACHE_TTL seconds (default 60)
```

Bench on an M-series laptop, `local[*]`, 1M rows -> 187,943 documents
(dedup + day window + 9 aggregates, sink to the in-memory fake):

| sink fake  | wall   | throughput   |
|------------|--------|--------------|
| opensearch | 3.54 s | 282k rows/s  |
| clickhouse | 3.67 s | 273k rows/s  |

Same Spark plan either way; the gap is the ClickHouse fake appending rows instead of overwriting a dict.

## Design notes

- Event ids are a hash of `(entity, type, source, ts)`, so a retried publish is the same event
  and `dropDuplicates` with a watermark removes it; no producer-side idempotence needed.
- One aggregation level (entity x tumbling window) keeps the streaming query in `update` mode;
  every micro-batch emits the running rollup for touched windows and upsert-by-`entity_id`
  makes replays and restarts idempotent. Index-per-day maps 1:1 onto the window.
- Streaming also archives decoded events to Parquet partitioned by day. Batch mode over that
  archive is the recovery path; the DAG runs it nightly so the index never depends on the
  streaming job having been healthy all day.
- OpenSearch, ClickHouse, Redis and Kafka sit behind small protocols (`SearchStore`, `Cache`,
  `Producer`) with in-memory fakes, so tests cover producer -> protobuf -> Spark -> sink -> cache -> API
  without containers. The ClickHouse tests also assert the generated DDL/insert/lookup SQL.
- ClickHouse sink: one `signals_daily` table, `ReplacingMergeTree(updated_at)`, `PARTITION BY day`,
  `ORDER BY (day, entity_id)`. The pipeline's upsert becomes a plain batched insert (one per
  micro-batch); a replay or restart inserts the same keys again and the background merge keeps the
  newest `updated_at`. Reads add `FINAL` so they see the collapsed row before the merge has run;
  the point lookup and history queries hit one partition (or a short `day IN` list) and the primary
  key, so `FINAL` costs little. `refresh` maps to `OPTIMIZE ... PARTITION ID 'YYYYMMDD' FINAL`, which
  the nightly verify step uses to force the merge for the day it just rebuilt. Day partitions map 1:1
  onto the window, mirroring index-per-day on the OpenSearch side.
- Redis cache: `CachedStore` wraps any sink; `/entities/{id}` reads `entity:{id}:{day}` first and fills
  it on a miss with a TTL. The sink write path evicts the entity-days it just upserted instead of writing
  through, because every micro-batch rewrites the running rollup for thousands of entities and almost
  none are read before the next batch replaces them; eviction keeps the cache small and a lookup right
  after a batch sees the new value rather than waiting out the TTL. History reads bypass the cache
  (`mget` across days). Hits/misses are exposed as `sf_cache_lookups_total`.
- Session timezone is pinned to UTC so window boundaries, index names, ClickHouse `Date` partitions
  and API `day` params agree; window timestamps go into ClickHouse tz-aware so the driver never
  re-interprets them in the process's local zone.

## Next

- Per-signal-type breakdown inside the document (needs a second aggregation, i.e. append mode).
- `from_protobuf` via the spark-protobuf jar instead of a Python UDF once the decode path
  dominates the profile.
- Alias rollover to retire old daily indices.
