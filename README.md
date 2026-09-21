# signalforge

Streaming aggregation of per-entity signal scores (product review signals, sensor readings, ...).
Protobuf events go through Kafka into a PySpark Structured Streaming job that dedups, windows and
rolls them up per (tenant, entity); the rollups land in OpenSearch (one index per day, one document per
entity) or, with `--sink clickhouse`, in a single ClickHouse `ReplacingMergeTree` table. Small tenants
share the daily index and are pinned to a shard by `_routing`; tenants named in `SF_DEDICATED_TENANTS`
get their own daily index, and a per-tenant quota caps how many entities a tenant may roll up per day.
A small FastAPI service serves point lookups from whichever sink is configured, optionally through a
Redis read-through cache. An Airflow DAG compacts each day's Parquet archive, rebuilds that day's
rollups from it, then rolls the read aliases forward and retires indices past retention.
`signalforge.capacity` turns an event rate and retention into shards, nodes and a monthly bill.

```
producer ──protobuf──> Kafka (redpanda) ──> Spark Structured Streaming ──upsert──> OpenSearch ──────> FastAPI
                                              │  decode UDF · dedup(event_id)   │  pooled: signals-YYYY-MM-DD  │  /tenants/{t}/entities/{id}
                                              │  window(1 day) · agg per        │    _routing = tenant         │  /tenants/{t}/entities/{id}/history
                                              │  (tenant, entity) · quota       │  dedicated: signals-{t}-YYYY-MM-DD
                                              │                                 │    _id = tenant:entity       │
                                              │                                 │  aliases signals, signals-{t}│
                                              │                                 └──> ClickHouse (SF_SINK)      │
                                              │                                      signals_daily             │
                                              │                                      (day, tenant, entity) ──evict──> Redis (REDIS_URL)
                                              └──append──> Parquet archive (day=…)                               entity:{t}:{id}:{day}, TTL
                                                              │
                                     Airflow (daily) ─────────┴─> compact ─> re-index ─> verify ─> rollover (alias window, retire)
```

The same transform code runs in both modes: `--mode stream` reads Kafka, `--mode batch` replays a
Parquet directory. Prometheus metrics are exposed by the Spark driver (`:9108`) and the API (`/metrics`).

## Run

```
make venv          # python3.9 venv + deps (needs Java 17 for pyspark)
make test          # 18 tests, no docker: pyspark local mode, in-memory OpenSearch/ClickHouse/Redis/Kafka fakes
make bench         # 1M synthetic rows (200 Zipf-sized tenants) through the batch path; SINK=clickhouse for that fake
make bench BENCH_ARGS="--dedicated t-000 --quota 2000"   # + per-index fan-out, routing keys, quota hits, doc size

make up            # redpanda + opensearch + clickhouse + redis via docker compose
make produce       # 5000 protobuf events (5% redelivered, 4 tenants) -> topic `signals`
make stream        # Kafka -> OpenSearch, keeps running; add --once to drain and exit
make api           # http://localhost:8000/entities/ent-0001  (SF_API_PORT to change)
make batch DAY=2026-09-05   # rebuild one day's index from data/archive
make airflow       # optional: installs airflow, runs `airflow standalone` with dags/

make stream SINK=clickhouse             # same job, rollups into ClickHouse `signals_daily`
make batch  SINK=clickhouse DAY=2026-09-05
make api    SINK=clickhouse             # or SF_SINK=clickhouse; SF_CLICKHOUSE_URL=http://localhost:8123
REDIS_URL=redis://localhost:6379 make api   # point lookups cached, SF_CACHE_TTL seconds (default 60)

SF_DEDICATED_TENANTS=acme SF_ROUTING_PARTITIONS=beta=4 SF_TENANT_QUOTA=beta=50000 SF_TENANT_QUOTA_DEFAULT=200000 \
  make stream      # acme gets signals-acme-<day>; beta is spread over 4 routing keys and capped at 50k entities/day
SF_RETENTION_DAYS=30 SF_INDEX_SHARDS=pooled=3,acme=6 make airflow   # rollover task: aliases + retirement + pre-create

python -m signalforge.capacity --events-per-sec 20000 --entities-per-day 2000000 --retention-days 30 \
  --doc-bytes 337 --dedicated-tenants 20 --dedicated-share 0.4 --node-type r6g.xlarge.search
```

Bench on an M-series laptop, `local[*]`, 1M rows over 200 tenants -> 184,100 documents
(dedup + day window + 9 aggregates per (tenant, entity), routing per document, sink to the in-memory fake):

| sink fake  | tenancy                          | wall   | throughput   |
|------------|----------------------------------|--------|--------------|
| opensearch | pooled                           | 3.79 s | 264k rows/s  |
| opensearch | t-000 dedicated, quota 2000/day  | 3.77 s | 266k rows/s (106,481 docs; 19 tenant-days capped) |
| clickhouse | pooled                           | 3.99 s | 250k rows/s  |

Same Spark plan either way; the gap is the ClickHouse fake appending rows instead of overwriting a dict.
Before tenancy (single group key, no routing) the same bench did 282k / 273k rows/s. Measured document
size is 337 bytes of JSON `_source`.

The capacity example above (20k events/s, 2M entities/day, 30 days, 1 replica, 20 dedicated tenants
holding 40% of the documents, r6g.xlarge) comes out ingest-bound at 10 data nodes, 1,302 shards
(130 per node, under the 25-per-GiB-heap guideline), 62 GiB provisioned and ~$3.1k/month at list price.
Giving 2,000 tenants a dedicated 90-day index each instead is shard-bound at thousands of nodes, which
is the number that argues for pooling small tenants behind `_routing`.

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
- Tenancy: the document key is `tenant:entity` and the event id hashes the tenant too, so two tenants
  reporting the same observation neither collide in a pooled index nor dedup into one event. Pooled
  tenants write and read with `routing=tenant`, so a tenant's GETs and bulk writes touch one shard
  (`SF_ROUTING_PARTITIONS=tenant=n` spreads a mid-sized tenant over `n` routing keys). Dedicated
  tenants get `prefix-<tenant>-<day>` with the default hash-by-id spread and their own shard count; that
  is the move for a tenant big enough to make a pooled shard hot. The quota counts distinct entities a
  tenant has rolled up per day in the driver (already-admitted entities always pass, since every
  micro-batch re-emits their running rollup); over-cap documents are dropped and counted in
  `sf_quota_dropped_total{tenant}`. Kafka messages are keyed by tenant so a tenant's events stay ordered.
- Lifecycle: `lifecycle.rollover` runs after the nightly verify. It pre-creates today's and tomorrow's
  index per family (pooled, and one per dedicated tenant) with the shard count from `SF_INDEX_SHARDS`,
  so index creation never happens on the streaming write path at midnight; moves each read alias
  (`signals`, `signals-<tenant>`) to the retention window in one `update_aliases` call; and only then
  deletes indices older than the window (`sf_indices_retired_total`). Re-running it is a no-op. On
  ClickHouse the same step drops day partitions; there are no aliases to move.
- Capacity: `signalforge.capacity` is a plain arithmetic model with every constant exposed. Documents
  per day = distinct (tenant, entity); on-disk bytes = `_source` x overhead; shards per daily index from
  a target shard size; total shards = per-day shards x (retention + 1) x (1 + replicas); nodes are the
  max of storage / EBS per node, shards / (heap x 25) and upserts x replicas / (vCPU x rate), with a
  floor of replicas + 1. Upserts per second are `min(events/s, entities per micro-batch)` because the
  sink re-writes each touched entity once per batch, not once per event. The bench prints the measured
  document size to feed in; the indexing rate per vCPU is the assumption to measure on a real domain.
- Session timezone is pinned to UTC so window boundaries, index names, ClickHouse `Date` partitions
  and API `day` params agree; window timestamps go into ClickHouse tz-aware so the driver never
  re-interprets them in the process's local zone.

## Next

- Per-signal-type breakdown inside the document (needs a second aggregation, i.e. append mode).
- `from_protobuf` via the spark-protobuf jar instead of a Python UDF once the decode path
  dominates the profile.
- Quota state survives a driver restart only via checkpoint replay; seed it from the index on start.
- Move a tenant between pooled and dedicated without a reindex (write to both, switch the alias).
