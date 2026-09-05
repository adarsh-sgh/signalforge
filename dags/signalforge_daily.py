"""Airflow DAG: compact yesterday's Parquet partition, rebuild its index, verify doc count."""
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from signalforge import backfill
from signalforge.config import settings
from signalforge.pipeline.job import build_spark
from signalforge.search.store import open_store


def _compact(ds: str, **_):
    return backfill.compact_day(build_spark("sf-compact"), ds)


def _reindex(ds: str, ti, **_):
    n = backfill.reindex_day(build_spark("sf-reindex"), ds, open_store(settings.opensearch_url, settings.index_prefix))
    ti.xcom_push(key="docs", value=n)
    return n


def _verify(ds: str, ti, **_):
    expected = ti.xcom_pull(task_ids="reindex_day", key="docs") or 0
    return backfill.verify_day(ds, open_store(settings.opensearch_url, settings.index_prefix), expected)


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
    compact >> reindex >> verify
