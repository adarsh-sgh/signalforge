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


settings = Settings()
