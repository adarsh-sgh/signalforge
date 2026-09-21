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


settings = Settings()
