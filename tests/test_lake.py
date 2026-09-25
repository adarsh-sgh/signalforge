"""Delta table behaviour that the rest of the design leans on: an added column lands without
rewriting history, a day partition can be replaced idempotently, and an old snapshot is readable.

Runs on a local filesystem path so no MinIO is needed; the only extra cost is the delta-spark jar,
which `spark.jars.packages` pulls once. `is_object_store` decides whether the s3a jars are added,
and scripts/smoke.sh runs the same code against MinIO over s3a.
"""
import pytest

from signalforge.config import Settings
from signalforge.lake.delta import (DELTA_PACKAGE, S3A_PACKAGES, DeltaLake, is_object_store,
                                    lake_packages, s3a_configs)
from signalforge.pipeline.job import build_spark, run_batch
from signalforge.search.store import InMemoryStore
from tests.conftest import D1, D2


@pytest.fixture(scope="module")
def delta_spark():
    s = build_spark("sf-delta-test", packages=[DELTA_PACKAGE],
                    configs=DeltaLake("/tmp/unused").configs()).newSession()
    s.sparkContext.setLogLevel("ERROR")
    yield s


def test_pipeline_writes_the_rollups_to_delta_and_a_day_can_be_replaced_in_place(
        delta_spark, cfg, archive, tmp_path):
    """The batch job feeds both legs: documents to the serving store and the same rows to the lake.
    Re-running one day must not double it."""
    lake = DeltaLake(str(tmp_path / "signals_daily"))
    store = InMemoryStore()

    assert run_batch(delta_spark, archive, store, cfg, lake=lake) == 4
    table = lake.read(delta_spark)
    assert table.count() == 4
    assert sorted(r["day"] for r in table.select("day").collect()) == [D1, D1, D2, D2]
    assert "mean_score" in table.columns and "tenant_id" in table.columns
    # `day` is the only partition column, which is what the Trino guard's rule keys on
    assert [f.name for f in delta_spark.read.format("delta").load(lake.path).schema][-1] == "day"

    # replaying one day rewrites just that partition: still 4 rows, and one more commit
    assert run_batch(delta_spark, archive, store, cfg, day=D2, lake=lake) == 2
    assert lake.read(delta_spark).count() == 4
    assert lake.read(delta_spark, day=D1).count() == 2
    versions = [c.version for c in lake.history(delta_spark)]
    assert versions == sorted(versions, reverse=True) and len(versions) == 2
    assert lake.latest(delta_spark).version == 1


def test_schema_evolution_and_time_travel(delta_spark, tmp_path):
    """v0 is the pre-stddev document shape; v1 adds the column with mergeSchema. Old rows read
    back as null, the old snapshot still reads with the old schema."""
    lake = DeltaLake(str(tmp_path / "evolving"))
    old = delta_spark.createDataFrame([(D1, "t-0", "ent-1", 3.5, 10)],
                                      "day string, tenant_id string, entity_id string, "
                                      "mean_score double, n long")
    new = delta_spark.createDataFrame([(D2, "t-0", "ent-1", 4.0, 12, 0.25)],
                                      "day string, tenant_id string, entity_id string, "
                                      "mean_score double, n long, stddev_score double")
    assert lake.write(old) == 1
    with pytest.raises(Exception):           # the added column is rejected without mergeSchema
        lake.write(new)
    assert lake.write(new, merge_schema=True) == 1

    latest = lake.read(delta_spark)
    assert "stddev_score" in latest.columns and latest.count() == 2
    by_day = {r["day"]: r["stddev_score"] for r in latest.select("day", "stddev_score").collect()}
    assert by_day == {D1: None, D2: 0.25}

    v0 = lake.read(delta_spark, version=0)
    assert "stddev_score" not in v0.columns and v0.count() == 1

    commits = lake.history(delta_spark)
    assert [c.version for c in commits] == [1, 0]
    assert all(c.operation == "WRITE" for c in commits)
    assert commits[0].timestamp_ms >= commits[1].timestamp_ms
    # timestampAsOf resolves to the commit that was current at that instant
    import datetime as dt

    as_of = dt.datetime.fromtimestamp(commits[1].timestamp_ms / 1000, dt.timezone.utc)
    assert lake.read(delta_spark, as_of=as_of).count() == 1


def test_object_store_paths_pull_the_s3a_jars_and_minio_configs():
    assert is_object_store("s3a://lake/signals_daily") and not is_object_store("/tmp/x")
    assert lake_packages("/tmp/x") == [DELTA_PACKAGE]
    assert lake_packages("s3a://lake/t") == [DELTA_PACKAGE] + list(S3A_PACKAGES)

    cfg = Settings(s3_endpoint="http://minio:9000", s3_access_key="k", s3_secret_key="s")
    conf = s3a_configs(cfg)
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"          # MinIO needs this
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "k"

    lake = DeltaLake("s3a://lake/signals_daily")
    assert "spark.sql.extensions" in lake.configs(cfg) and "s3a" in lake.register_in_trino()
    assert DeltaLake.from_settings(Settings(lake_path="")) is None
