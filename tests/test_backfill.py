import glob
import os

import pytest

from signalforge import backfill
from signalforge.search.store import InMemoryStore
from tests.conftest import D1, EVENTS, write_events


def test_compact_reindex_verify(spark, cfg):
    write_events(cfg.archive_dir, EVENTS, files_per_day=4)
    day_dir = backfill.day_path(cfg.archive_dir, D1)
    assert backfill.compact_day(spark, D1, cfg) == 4
    assert len(glob.glob(os.path.join(day_dir, "*.parquet"))) == 1

    store = InMemoryStore()
    assert backfill.reindex_day(spark, D1, store, cfg) == 2
    assert store.get("test-%s" % D1, "default:ent-1", "default")["n"] == 3  # compaction kept every row
    assert backfill.verify_day(D1, store, expected=2, cfg=cfg) == 2
    with pytest.raises(RuntimeError):
        backfill.verify_day(D1, store, expected=3, cfg=cfg)
    assert backfill.compact_day(spark, "2020-01-01", cfg) == 0  # missing partition is a no-op


def test_dag_wiring():
    pytest.importorskip("airflow")
    from dags.signalforge_daily import dag

    assert [t.task_id for t in dag.topological_sort()] == ["compact_day", "reindex_day", "verify_day"]
    assert dag.schedule_interval == "0 2 * * *" and not dag.catchup
