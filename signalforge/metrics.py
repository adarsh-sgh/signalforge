"""Prometheus metrics. The Spark driver and the API each expose their own registry."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS_DECODED = Counter("sf_events_decoded_total", "Protobuf events decoded")
EVENTS_DROPPED = Counter("sf_events_dropped_total", "Undecodable or invalid events dropped")
DOCS_UPSERTED = Counter("sf_docs_upserted_total", "Documents upserted into the sink", ["index"])
BATCH_SECONDS = Histogram("sf_batch_seconds", "Wall time per micro-batch / batch run")
BATCH_ROWS = Gauge("sf_last_batch_rows", "Input rows in the most recent batch")

HTTP_REQUESTS = Counter("sf_http_requests_total", "API requests", ["path", "status"])
HTTP_LATENCY = Histogram("sf_http_latency_seconds", "API latency", ["path"])
CACHE_LOOKUPS = Counter("sf_cache_lookups_total", "Point-lookup cache results", ["result"])


def serve(port: int) -> None:
    start_http_server(port)
