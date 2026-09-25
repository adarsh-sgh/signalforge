# signalforge

Streaming aggregation of per-entity signal scores (product review signals, sensor readings, ...).
Protobuf events go through Redpanda (Kafka API) into two independent consumers: a PySpark Structured
Streaming job that dedups, windows and rolls them up per (tenant, entity), and a PyFlink job that
scores the same stream for anomalies with a rolling z-score or EWMA baseline. The Spark rollups land
in OpenSearch (one index per day, one document per entity) or, with `--sink clickhouse`, in a single
ClickHouse `ReplacingMergeTree` table, and -- when `SF_LAKE_PATH` is set -- also in a Delta table on
S3-compatible storage that Trino queries. Trino sits behind an admission guard that refuses or
throttles abusive statements before they reach the coordinator. Small tenants
share the daily index and are pinned to a shard by `_routing`; tenants named in `SF_DEDICATED_TENANTS`
get their own daily index, and a per-tenant quota caps how many entities a tenant may roll up per day.
A small FastAPI service serves point lookups from whichever sink is configured, optionally through a
Redis read-through cache. An Airflow DAG compacts each day's Parquet archive, rebuilds that day's
rollups and Delta partition from it, checks every dataset against its freshness SLO and heals what
breached, then rolls the read aliases forward and retires indices past retention.
`signalforge.capacity` turns an event rate and retention into shards, nodes and a monthly bill.

```
producer ──protobuf──> Redpanda (Kafka API) ──> Spark Structured Streaming ──upsert──> OpenSearch ──────> FastAPI
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
                                     Airflow (daily) ─────────┴─> compact ─> re-index ─> verify ─> sla_check ─> rollover
                                                                                            (heal breaches)  (alias window, retire)

Redpanda `signals` ──> PyFlink anomaly job ──> `signal_anomalies` (JSON)
                         │  decode · watermarks (bounded out-of-orderness)
                         │  tumbling window per (tenant, entity): n/sum/min/max
                         │  keyed detector state: rolling z-score | EWMA
                         └──late (allowed lateness 0)──> `signal_late`

Spark rollups ──append/replaceWhere──> Delta table on S3 (partition by day) ──> Trino (delta_lake, file metastore)
                                                                                  ^
                                        analyst ──POST /v1/queries──> admission guard ──┘
                                                   403 rule · 429 Retry-After · jsonl audit
```

The same transform code runs in both modes: `--mode stream` reads Kafka, `--mode batch` replays a
Parquet directory. Prometheus metrics are exposed by the Spark driver (`:9108`) and the API (`/metrics`).

## Run

```
make venv          # python3.9 venv + deps (needs Java 17 for pyspark)
make test          # 43 tests, no docker: pyspark local mode, a real delta table on a tmp path,
                   #   in-memory OpenSearch/ClickHouse/Redis/Kafka/Trino fakes (the DAG-wiring test
                   #   skips until `make airflow` has installed airflow)
make flink-test    # 5 more on a local Flink MiniCluster, inside a linux/amd64 image
make bench         # 1M synthetic rows (200 Zipf-sized tenants) through the batch path; SINK=clickhouse for that fake
make bench BENCH_ARGS="--dedicated t-000 --quota 2000"   # + per-index fan-out, routing keys, quota hits, doc size

make up            # redpanda + opensearch + clickhouse + redis + s3 + trino + flink via docker compose
make smoke         # the whole stack end to end; prints every number in the table below
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

make flink-job                              # submit the anomaly job to the compose cluster
make flink-job FLINK_ARGS="--mode ewma --threshold 2.5 --window-ms 5000"
SF_ANOMALY_METRIC=event_rate make flink-job   # watch volume instead of mean score

SF_LAKE_PATH=s3a://lake/signals_daily make stream SINK=clickhouse   # + the Delta leg on S3
make lake-register                          # give that table a name Trino can query
make trino-sql SQL="SELECT day, count(*) FROM delta.signals.signals_daily GROUP BY day"
make guard                                  # admission service on :8010 in front of Trino
curl -s localhost:8010/v1/queries -H 'x-trino-user: analyst' -d \
  '{"sql":"SELECT entity_id FROM delta.signals.signals_daily WHERE day = '"'"'2026-09-25'"'"' LIMIT 5"}'
curl -s localhost:8010/v1/summary            # decisions, bytes refused, bytes actually scanned
SF_TRINO_MAX_SCAN_BYTES=2097152 SF_TRINO_MAX_CONCURRENT=2 SF_TRINO_AUDIT_FILE=data/audit.jsonl make guard

SF_SLA="archive:5400,lake:5400:1000,serving:3600" make airflow   # sla_check heals breaches
python scripts/e2e.py sla --day 2026-09-25 --sink clickhouse --heal

python -m signalforge.capacity --events-per-sec 20000 --entities-per-day 2000000 --retention-days 30 \
  --doc-bytes 337 --dedicated-tenants 20 --dedicated-share 0.4 --node-type r6g.xlarge.search
```

## Measured

Everything below comes from `make smoke` on one Apple M-series laptop with the full compose stack
(Redpanda, SeaweedFS S3, ClickHouse, Redis, Trino 476, Flink 2.1.3) running beside the driver, so the
numbers are a laptop's numbers, not a cluster's.

| leg | measured |
|-----|----------|
| producer -> Redpanda | 200,000 protobuf events in 0.82 s, **245k events/s**, one producer |
| Redpanda -> Spark -> ClickHouse **and** Delta on S3 | 416,504 events replayed in 13.4 s, **31.0k events/s**, 32,000 rollup documents |
| PyFlink anomaly job | **6.1k events/s** at parallelism 1, **12.2k/s** at parallelism 4 (see the caveat below) |
| nightly `replaceWhere` re-index | lake partitions 31,988 -> 15,994 and 48,018 -> 16,006 rows, ClickHouse unchanged |
| Trino over the Delta table | same 15,994 / 16,006 rollups per day as the serving store |
| Trino IO estimate, full scan vs one day | 1.7 MiB vs 859.7 KiB -- the guard's partition rule is worth ~2x here |
| admission guard, 8-query workload | 3 admitted, 5 refused; **12.7 MiB of estimated scan refused** vs 1.2 MiB actually processed |
| freshness sweep | 2 stale datasets (309 s and 274 s against a 60 s budget), both healed by rebuilding 16,006 rows |

The Flink number carries a real caveat: apache-flink publishes x86_64-only Linux wheels, so on this
arm64 laptop the job runs under emulation and the per-record Python path pays for it. It is a floor,
not a ceiling, and the shape (roughly 2x from 1 to 4 slots) is the informative part. The p=4 run
drained a backlog as well as the 200k it was given, so its rate is if anything conservative.

One anomaly run, end to end: 14 quiet 2-second windows at mean 3.5 then one window at 12.0 produced
exactly one anomaly on `signal_anomalies` (`value=12.00 baseline=3.55 z=169.0 detector=zscore`), and
the straggler stamped 28 windows in the past arrived on `signal_late` instead of silently changing a
closed window.

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
- Anomaly detection is deliberately two pieces: `signalforge.anomaly` is pure -- `update(state, stat,
  cfg) -> (state, anomaly?)` over a picklable `DetectorState` -- and `signalforge.flink.anomaly_job`
  is the wiring. That is what lets the same detector run inside a Flink keyed operator (state as JSON
  in `ValueState`), in a batch replay, and in tests, and it is why one test can assert the Flink job
  emits exactly what `Detector.run(window_stats(...))` does on the same input. Two baselines:
  `zscore` keeps the last `history` window values (bounded memory, reacts sharply), `ewma` keeps a
  smoothed mean and variance (O(1) state, forgets regime changes faster). Every window is folded into
  the baseline whether or not it alerted, so a sustained shift alerts once and then becomes the new
  normal rather than paging every window; coming back to the old level alerts again.
- The Flink window runs with `allowed_lateness = 0` and a late side output rather than a lateness
  budget. With a budget, a straggler re-fires an already-closed window *after* later windows have
  already advanced the detector's baseline, so the baseline would see windows out of order and its
  variance would be wrong. Late events go to `signal_late` and are picked up by the nightly Spark
  replay of the Parquet archive, which recomputes the day from scratch and does not care about order.
- PyFlink's `TypeInformation` objects cannot be cloudpickled once they have been handed to the JVM,
  so the late-data `OutputTag` gets a freshly built type each time `anomaly_pipeline` runs. And
  `build_env` pins `RuntimeExecutionMode.STREAMING`: `AUTOMATIC` runs a bounded source in batch mode,
  where watermarks never advance mid-stream and nothing is ever late.
- Lakehouse and serving store are separate on purpose. ClickHouse keeps one collapsed row per
  (day, tenant, entity) for millisecond point lookups; the Delta table keeps every commit so a query
  can time-travel to what a dashboard saw yesterday (`versionAsOf`, `timestampAsOf`) and a new column
  can be added with `mergeSchema` without rewriting history -- old files keep the old schema and read
  back as nulls. The streaming leg *appends* (it is a log), so a full replay duplicates rows on the
  lake while ReplacingMergeTree keeps the serving store correct; the nightly re-index writes the day
  with `replaceWhere day = '...'`, which is what reconciles the two. The numbers above show that
  round trip: 48,018 rows collapsing back to 16,006.
- The Delta connector runs on a *file* metastore (`hive.metastore=file`) rather than a Hive Metastore
  container, so the only extra step is `register_table` to give the table Spark wrote a name. One
  fewer service to run locally, and it makes the point that the catalog is not the interesting part.
- Query governance is split into what can be decided offline and what needs the coordinator.
  `signalforge.trino.plan` parses with sqlglot (Trino dialect) and answers: which tables, which of
  their columns are *pruned* on, is there a bare `*` without a LIMIT, is there a join with no
  condition. "Pruned on" means a sargable predicate -- a bare column against a literal with `=`,
  `IN`, `BETWEEN` or an inequality -- because `WHERE day <> '...'` and `WHERE substr(day,1,7) = '...'`
  mention the partition column and still read every partition. A star inside a function is not a
  `SELECT *`, or `count(*)` would be the most-refused query in the workload.
- The byte budget uses Trino's own answer, not a guess: `EXPLAIN (TYPE IO, FORMAT JSON)` returns a
  per-input-table `estimate.outputSizeInBytes`, which is planning only and reads no data. It is
  genuinely an estimate and is absent (NaN) when the connector has no statistics, so a missing
  estimate never becomes a rejection -- that rule simply does not fire. The estimate is taken
  *before* the structural rules so a refusal can be costed, which is what makes "bytes the guard
  refused" a number rather than a count.
- Refusal and throttling are different answers. A query that can never be allowed as written
  (no partition predicate, over budget, cross join, `SELECT *` with no LIMIT, not a read) is 403 with
  the rule that fired; a user who is simply at their concurrency cap is 429 with `Retry-After`, and
  their slot is released in a `finally` so a query that fails inside Trino does not leak capacity.
  Every decision and every outcome goes to an append-only JSONL audit, so the estimates can be
  checked against `processedBytes` afterwards and the budget calibrated instead of guessed.
- Self-healing leans on every sink already being idempotent: upsert-by-id on OpenSearch,
  `ReplacingMergeTree` on ClickHouse, `replaceWhere` on Delta. That makes "re-run the day" a safe
  default repair, so `sla_check` can take it without a human. It is bounded on purpose --
  `max_attempts` per (dataset, day) with a backoff, then the breach escalates and fails the task. A
  pipeline that silently retries a permanently broken day is worse than one that pages someone.
  A handler that raises is one failed attempt, not an aborted sweep, so one broken dataset does not
  hide the others.
- Freshness is measured from each dataset's own write record, not a side table: the newest Parquet
  file's mtime for the archive, the newest Delta commit for the lake, and the row count plus the
  caller's write time for the serving store. If the nightly replay never ran, the newest commit is
  yesterday's, and that is exactly the signal wanted.

## Not done

- No Ray Serve endpoint and no MLflow-registered detector. The detector is six numbers of state and a
  z-score, so serving it over HTTP would be ceremony rather than engineering; a learned detector
  would change that.
- No Superset dashboard definition. Trino is the query layer and `make trino-sql` exercises it; a
  Superset YAML that nothing in the repo starts would be a claim without code behind it.
- No Apache Ranger. The guard does admission control (which statements run), not row/column
  authorization, and wiring a Ranger plugin needs a Java plugin and a Ranger admin service.
- Governance is an admission *proxy*, not a Trino event listener. An event listener plugin would see
  every query including ones submitted straight to the coordinator, but it is a Java SPI, and it
  observes after the fact -- it cannot refuse. The proxy can refuse, at the cost of being bypassable
  by anyone who talks to :8080 directly.
- `sla_check` keeps its attempt counters in the scheduler process, so a scheduler restart resets a
  day to attempt 1. Wrong direction for a hard cap, right direction for not wedging a pipeline.
- The PyFlink late-data path is verified against Redpanda (`scripts/e2e.py anomaly`), not in the
  MiniCluster tests: a bounded `from_collection` source never advances a watermark mid-stream, so
  nothing in it is ever late. The tests assert the window/detector semantics and that the late stream
  is wired and empty for in-order input.

## Next

- Per-signal-type breakdown inside the document (needs a second aggregation, i.e. append mode).
- `from_protobuf` via the spark-protobuf jar instead of a Python UDF once the decode path
  dominates the profile.
- Quota state survives a driver restart only via checkpoint replay; seed it from the index on start.
- Move a tenant between pooled and dedicated without a reindex (write to both, switch the alias).
- Feed `signal_anomalies` back into the serving store so the API can answer "was this entity
  anomalous today" without a Trino query.
- Seed the Flink detector state from the last day of rollups on start, so a fresh job does not spend
  `min_samples` windows warming up per key.
- Have the guard learn its budget: the audit already has estimate-vs-actual pairs per query shape.
