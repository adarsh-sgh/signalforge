"""Delta table on S3-compatible object storage (SeaweedFS locally, S3 in a real deployment).

The pipeline writes every rollup document twice: into ClickHouse/OpenSearch for point lookups
(milliseconds, one row per entity-day) and, append-only, into `signals_daily` on the lake for
ad-hoc SQL through Trino. The split is deliberate: the serving store is keyed and small, the
lake keeps every commit so a query can time-travel to what a dashboard saw yesterday.

Only `day` partitions the table, matching the daily index / ClickHouse partition, so the
Trino guard's "must have a predicate on the partition column" rule has something to check.
"""
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

from signalforge.config import Settings, settings

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.1"
# hadoop-aws must match the hadoop version pyspark 3.5 bundles, and s3a needs the sdk bundle
S3A_PACKAGES = ("org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262")
PARTITION_COLUMNS = ("day",)


def delta_configs() -> Dict[str, str]:
    return {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}


def s3a_configs(cfg: Settings = settings) -> Dict[str, str]:
    """SeaweedFS and MinIO need path-style access and an explicit endpoint; S3 proper ignores both."""
    return {"spark.hadoop.fs.s3a.endpoint": cfg.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": cfg.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": cfg.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(cfg.s3_endpoint.startswith("https")).lower(),
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"}


def lake_packages(path: str) -> List[str]:
    """s3a jars are only worth fetching when the table actually lives on object storage."""
    pkgs = [DELTA_PACKAGE]
    if is_object_store(path):
        pkgs.extend(S3A_PACKAGES)
    return pkgs


def is_object_store(path: str) -> bool:
    return urlparse(path).scheme in ("s3a", "s3", "s3n")


@dataclass(frozen=True)
class Commit:
    version: int
    timestamp_ms: int
    operation: str
    rows: Optional[int] = None


class DeltaLake:
    """Append-only writer plus the two read paths Trino and the SLA check need."""

    def __init__(self, path: str, table: str = "signals_daily") -> None:
        if not path:
            raise ValueError("lake path is empty; set SF_LAKE_PATH")
        self.path = path.rstrip("/")
        self.table = table

    @classmethod
    def from_settings(cls, cfg: Settings = settings) -> Optional["DeltaLake"]:
        """None when no lake is configured, so the pipeline can skip the leg entirely."""
        return cls(cfg.lake_path, cfg.lake_table) if cfg.lake_path else None

    def configs(self, cfg: Settings = settings) -> Dict[str, str]:
        conf = delta_configs()
        if is_object_store(self.path):
            conf.update(s3a_configs(cfg))
        return conf

    def write(self, docs: DataFrame, mode: str = "append", merge_schema: bool = False,
              replace_where: Optional[str] = None) -> int:
        """Append one batch of documents. `merge_schema` is what makes an added column land
        without rewriting history: old files keep the old schema and read back as nulls."""
        n = docs.count()
        writer = docs.write.format("delta").mode(mode).partitionBy(*PARTITION_COLUMNS)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if replace_where:
            writer = writer.option("replaceWhere", replace_where)
        writer.save(self.path)
        return n

    def rewrite_day(self, docs: DataFrame, day: str) -> int:
        """Nightly re-index path: swap one day partition atomically instead of appending it twice,
        so replaying a day is idempotent on the lake the way upsert-by-id is on the serving store."""
        return self.write(docs, mode="overwrite", merge_schema=True,
                          replace_where="day = '%s'" % day)

    def read(self, spark: SparkSession, version: Optional[int] = None,
             as_of: Optional[dt.datetime] = None, day: Optional[str] = None) -> DataFrame:
        """Latest snapshot, or `versionAsOf` / `timestampAsOf` for time travel."""
        reader = spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif as_of is not None:
            # Delta parses the string in the session timezone (pinned to UTC) and does not round
            # up, so the milliseconds matter: truncating to the second lands before the commit.
            utc = as_of.astimezone(dt.timezone.utc) if as_of.tzinfo else as_of
            reader = reader.option("timestampAsOf", utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        df = reader.load(self.path)
        return df.where(df.day == day) if day else df

    def history(self, spark: SparkSession, limit: int = 20) -> List[Commit]:
        from delta.tables import DeltaTable

        # unix_millis in SQL rather than the collected datetime: pyspark renders a Spark timestamp
        # as a naive datetime in the *driver's* local zone, which would shift the epoch.
        rows = (DeltaTable.forPath(spark, self.path).history(limit)
                .selectExpr("version", "unix_millis(timestamp) as ts_ms", "operation",
                            "operationMetrics").collect())
        out = []
        for r in rows:
            rows_written = (r["operationMetrics"] or {}).get("numOutputRows")
            out.append(Commit(int(r["version"]), int(r["ts_ms"]), r["operation"],
                              int(rows_written) if rows_written is not None else None))
        return out

    def latest(self, spark: SparkSession) -> Optional[Commit]:
        """Newest commit, or None for a path with no table yet. Drives the freshness SLO."""
        try:
            hist = self.history(spark, limit=1)
        except Exception:
            return None
        return hist[0] if hist else None

    def register_in_trino(self, schema: str = "signals") -> str:
        """Trino's Delta connector on a file metastore needs the table pointed at explicitly."""
        return ("CALL delta.system.register_table(schema_name => '%s', table_name => '%s', "
                "table_location => '%s')" % (schema, self.table, self.path))
