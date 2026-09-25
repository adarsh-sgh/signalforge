"""Delta Lake leg of the pipeline: the same rollup documents that go to the low-latency
serving store are also appended to a Delta table on S3-compatible storage, which is what
Trino queries."""
from signalforge.lake.delta import DeltaLake, delta_configs, s3a_configs  # noqa: F401
