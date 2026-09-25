"""Prometheus metrics. The Spark driver and the API each expose their own registry."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS_DECODED = Counter("sf_events_decoded_total", "Protobuf events decoded")
EVENTS_DROPPED = Counter("sf_events_dropped_total", "Undecodable or invalid events dropped")
DOCS_UPSERTED = Counter("sf_docs_upserted_total", "Documents upserted into the sink", ["index"])
QUOTA_DROPPED = Counter("sf_quota_dropped_total", "Documents dropped by the per-tenant entity quota", ["tenant"])
LAKE_ROWS = Counter("sf_lake_rows_total", "Rows appended to the Delta table on the lake")
INDICES_RETIRED = Counter("sf_indices_retired_total", "Daily indices deleted past retention", ["alias"])
BATCH_SECONDS = Histogram("sf_batch_seconds", "Wall time per micro-batch / batch run")
BATCH_ROWS = Gauge("sf_last_batch_rows", "Input rows in the most recent batch")

DATASET_LAG = Gauge("sf_dataset_lag_seconds", "Age of the newest write per dataset (-1 = nothing)",
                    ["dataset"])
SLA_BREACHES = Counter("sf_sla_breaches_total", "Freshness SLO breaches", ["dataset", "kind"])
HEAL_ACTIONS = Counter("sf_heal_actions_total", "Self-healing actions taken", ["dataset", "action"])

TRINO_DECISIONS = Counter("sf_trino_decisions_total", "Admission decisions", ["verdict", "rule"])
TRINO_SCAN_BLOCKED = Counter("sf_trino_scan_bytes_blocked_total",
                             "Estimated bytes of table scan the guard refused")
TRINO_SCANNED = Counter("sf_trino_scan_bytes_total", "Bytes Trino actually processed for admitted queries")
TRINO_INFLIGHT = Gauge("sf_trino_inflight_queries", "Queries in flight per user", ["user"])
TRINO_QUERY_SECONDS = Histogram("sf_trino_query_seconds", "Wall time of an admitted query")

HTTP_REQUESTS = Counter("sf_http_requests_total", "API requests", ["path", "status"])
HTTP_LATENCY = Histogram("sf_http_latency_seconds", "API latency", ["path"])
CACHE_LOOKUPS = Counter("sf_cache_lookups_total", "Point-lookup cache results", ["result"])


def serve(port: int) -> None:
    start_http_server(port)
