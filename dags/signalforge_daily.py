"""Airflow DAG: compact yesterday's Parquet partition, rebuild its index and the day's Delta
partition, verify doc count, check every dataset against its freshness SLO and heal what breached,
then roll the read aliases forward and retire indices past retention.

`sla_check` is the self-healing step: it probes the archive, the lake and the serving store, and
re-runs the day's backfill for whatever is missing, stale or short. It runs *after* verify so the
normal path has already had its chance, and it is bounded (`Slo.max_attempts`) so a permanently
broken day escalates instead of looping. Every task is idempotent, so a retry is always safe."""
import datetime as dt
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

from signalforge import backfill, lifecycle, sla
from signalforge.config import settings
from signalforge.lake.delta import DeltaLake
from signalforge.pipeline.job import build_spark, build_spark_for
from signalforge.search.store import open_store
from signalforge.tenancy import Router, parse_map


def _store():
    return open_store(settings.opensearch_url, settings.index_prefix)


def _compact(ds: str, **_):
    return backfill.compact_day(build_spark("sf-compact"), ds)


def _reindex(ds: str, ti, **_):
    n = backfill.reindex_day(build_spark_for(settings, "sf-reindex"), ds, _store(),
                             lake=DeltaLake.from_settings(settings))
    ti.xcom_push(key="docs", value=n)
    return n


def _sla_check(ds: str, ti, **_):
    """Probe every dataset for the day just rebuilt and heal what breached."""
    spark = build_spark_for(settings, "sf-sla")
    lake = DeltaLake.from_settings(settings)
    store, router = _store(), Router.from_settings(settings)
    now_ms = int(time.time() * 1000)

    readings = [sla.archive_freshness(settings.archive_dir, ds),
                sla.store_freshness(store, router.day_indices(ds), ds, updated_at_ms=now_ms)]
    if lake is not None:
        readings.append(sla.lake_freshness(lake, spark, ds))

    def rebuild(breach):
        return backfill.reindex_day(spark, breach.day, store, lake=lake)

    slos = sla.slos_from_settings(settings)
    healer = PER_DAG_HEALER.setdefault(ds, sla.Healer(slos, {sla.BACKFILL: rebuild}))
    report = sla.sweep(readings, slos, healer, now_ms=now_ms)
    ti.xcom_push(key="sla", value=report)
    if report["escalated"]:
        raise RuntimeError("SLO breach could not be healed: %s" % "; ".join(report["escalated"]))
    return report


# attempt counters live for the life of the scheduler process, which is what bounds the retries
# within one day's runs; a fresh process starts a day at attempt 1, which is the safe direction
PER_DAG_HEALER = {}


def _verify(ds: str, ti, **_):
    expected = ti.xcom_pull(task_ids="reindex_day", key="docs") or 0
    return backfill.verify_day(ds, open_store(settings.opensearch_url, settings.index_prefix), expected)


def _rollover(ds: str, **_):
    rep = lifecycle.rollover(open_store(settings.opensearch_url), Router.from_settings(settings), ds,
                             settings.retention_days, parse_map(settings.index_shards))
    return {"created": rep.created, "deleted": rep.deleted}


with DAG(
    dag_id="signalforge_daily",
    schedule="0 2 * * *",
    start_date=dt.datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=5)},
    tags=["signalforge"],
) as dag:
    compact = PythonOperator(task_id="compact_day", python_callable=_compact)
    reindex = PythonOperator(task_id="reindex_day", python_callable=_reindex)
    verify = PythonOperator(task_id="verify_day", python_callable=_verify)
    sla_check = PythonOperator(task_id="sla_check", python_callable=_sla_check)
    rollover = PythonOperator(task_id="rollover", python_callable=_rollover)
    compact >> reindex >> verify >> sla_check >> rollover
