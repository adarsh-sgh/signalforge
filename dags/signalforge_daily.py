"""Airflow DAG: compact yesterday's Parquet partition, rebuild its index, verify doc count, then roll the
read aliases forward and retire indices past retention."""
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from signalforge import backfill, lifecycle
from signalforge.config import settings
from signalforge.pipeline.job import build_spark
from signalforge.search.store import open_store
from signalforge.tenancy import Router, parse_map


def _compact(ds: str, **_):
    return backfill.compact_day(build_spark("sf-compact"), ds)


def _reindex(ds: str, ti, **_):
    n = backfill.reindex_day(build_spark("sf-reindex"), ds, open_store(settings.opensearch_url, settings.index_prefix))
    ti.xcom_push(key="docs", value=n)
    return n


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
    rollover = PythonOperator(task_id="rollover", python_callable=_rollover)
    compact >> reindex >> verify >> rollover
