import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get("SF_" + name, default)


@dataclass
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "signals"))
    opensearch_url: str = field(default_factory=lambda: _env("OPENSEARCH_URL", "http://localhost:9200"))
    index_prefix: str = field(default_factory=lambda: _env("INDEX_PREFIX", "signals"))
    archive_dir: str = field(default_factory=lambda: _env("ARCHIVE_DIR", "data/archive"))
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", "data/checkpoints"))
    window: str = field(default_factory=lambda: _env("WINDOW", "1 day"))
    watermark: str = field(default_factory=lambda: _env("WATERMARK", "10 minutes"))
    metrics_port: int = field(default_factory=lambda: int(_env("METRICS_PORT", "9108")))
    api_port: int = field(default_factory=lambda: int(_env("API_PORT", "8000")))
    sink: str = field(default_factory=lambda: _env("SINK", "opensearch"))
    clickhouse_url: str = field(default_factory=lambda: _env("CLICKHOUSE_URL", "http://localhost:8123"))
    # REDIS_URL (no SF_ prefix, the conventional name); empty disables the cache
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", ""))
    cache_ttl: int = field(default_factory=lambda: int(_env("CACHE_TTL", "60")))
    # tenancy: comma list of tenants with their own daily index; "tenant=n" maps (routing partitions,
    # max entities per day); a default quota of 0 means unlimited
    dedicated_tenants: str = field(default_factory=lambda: _env("DEDICATED_TENANTS", ""))
    routing_partitions: str = field(default_factory=lambda: _env("ROUTING_PARTITIONS", ""))
    tenant_quota: str = field(default_factory=lambda: _env("TENANT_QUOTA", ""))
    tenant_quota_default: int = field(default_factory=lambda: int(_env("TENANT_QUOTA_DEFAULT", "0")))
    retention_days: int = field(default_factory=lambda: int(_env("RETENTION_DAYS", "30")))
    # primaries for indices the lifecycle step pre-creates: "pooled=3,acme=6" (missing = mapping default)
    index_shards: str = field(default_factory=lambda: _env("INDEX_SHARDS", ""))
    # flink anomaly detection
    anomaly_mode: str = field(default_factory=lambda: _env("ANOMALY_MODE", "zscore"))
    anomaly_metric: str = field(default_factory=lambda: _env("ANOMALY_METRIC", "mean_score"))
    anomaly_threshold: float = field(default_factory=lambda: float(_env("ANOMALY_THRESHOLD", "3.0")))
    anomaly_min_samples: int = field(default_factory=lambda: int(_env("ANOMALY_MIN_SAMPLES", "5")))
    anomaly_history: int = field(default_factory=lambda: int(_env("ANOMALY_HISTORY", "30")))
    anomaly_alpha: float = field(default_factory=lambda: float(_env("ANOMALY_ALPHA", "0.3")))
    anomaly_min_stddev: float = field(default_factory=lambda: float(_env("ANOMALY_MIN_STDDEV", "0.05")))
    anomaly_window_ms: int = field(default_factory=lambda: int(_env("ANOMALY_WINDOW_MS", "60000")))
    # how far behind the newest event a straggler may arrive before the window closes on it
    anomaly_lateness_ms: int = field(default_factory=lambda: int(_env("ANOMALY_LATENESS_MS", "5000")))
    anomaly_topic: str = field(default_factory=lambda: _env("ANOMALY_TOPIC", "signal_anomalies"))
    anomaly_late_topic: str = field(default_factory=lambda: _env("ANOMALY_LATE_TOPIC", "signal_late"))
    # delta lake on s3-compatible storage; empty lake_path disables the lake leg
    lake_path: str = field(default_factory=lambda: _env("LAKE_PATH", ""))
    lake_table: str = field(default_factory=lambda: _env("LAKE_TABLE", "signals_daily"))
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "signalforge"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "signalforge"))
    # trino query layer + admission guard
    trino_url: str = field(default_factory=lambda: _env("TRINO_URL", "http://localhost:8080"))
    trino_catalog: str = field(default_factory=lambda: _env("TRINO_CATALOG", "delta"))
    trino_schema: str = field(default_factory=lambda: _env("TRINO_SCHEMA", "signals"))
    trino_guard_port: int = field(default_factory=lambda: int(_env("TRINO_GUARD_PORT", "8010")))
    trino_max_scan_bytes: int = field(default_factory=lambda: int(_env("TRINO_MAX_SCAN_BYTES", str(5 << 30))))
    trino_max_concurrent_per_user: int = field(default_factory=lambda: int(_env("TRINO_MAX_CONCURRENT", "3")))
    trino_policy_file: str = field(default_factory=lambda: _env("TRINO_POLICY_FILE", ""))
    trino_audit_file: str = field(default_factory=lambda: _env("TRINO_AUDIT_FILE", ""))
    # freshness SLOs: "dataset:max_lag_seconds[:min_rows[:heal]]" per dataset
    sla: str = field(default_factory=lambda: _env("SLA", "archive:5400,lake:5400,serving:5400"))


settings = Settings()
