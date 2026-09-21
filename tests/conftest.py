import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

if "JAVA_HOME" not in os.environ and os.path.isdir("/opt/homebrew/opt/openjdk@17"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
# keep airflow's scratch files (if installed) inside the repo, not ~/airflow
os.environ.setdefault("AIRFLOW_HOME", os.path.join(os.path.dirname(os.path.dirname(__file__)), "airflow"))

from signalforge.config import Settings  # noqa: E402
from signalforge.events.codec import make_event_id  # noqa: E402
from signalforge.pipeline.job import build_spark  # noqa: E402

D1, D2 = "2026-09-03", "2026-09-04"
_MS = 86_400_000
_T1 = 1788393600000  # 2026-09-03T00:00:00Z


def ev(entity, stype, score, source, offset_ms, event_id=None, tenant=None):
    """tenant=None leaves the column out, like archives written before tenancy existed."""
    ts = _T1 + offset_ms
    e = {"event_id": event_id or make_event_id(entity, stype, source, ts, tenant or "default"), "entity_id": entity,
         "signal_type": stype, "score": score, "source": source, "ts": ts}
    if tenant is not None:
        e["tenant_id"] = tenant
    return e


# Two entities over two days; the last row is an exact redelivery of the first.
EVENTS = [
    ev("ent-1", "review", 4.0, "web", 1_000),
    ev("ent-1", "rating", 2.0, "mobile", 3_600_000),
    ev("ent-1", "review", 5.0, "web", 7_200_000),
    ev("ent-2", "click", 1.0, "partner", 500),
    ev("ent-1", "review", 3.0, "web", _MS + 60_000),
    ev("ent-2", "return", 0.0, "web", _MS + 90_000),
    ev("ent-1", "review", 4.0, "web", 1_000),
]


def write_events(path, rows, files_per_day=1):
    """Hive-partitioned layout (day=YYYY-MM-DD/*.parquet), same as the streaming archive writes."""
    by_day = {}
    for r in rows:
        day = dt.datetime.utcfromtimestamp(r["ts"] / 1000).date().isoformat()
        by_day.setdefault(day, []).append(r)
    for day, drows in by_day.items():
        d = os.path.join(path, "day=%s" % day)
        os.makedirs(d, exist_ok=True)
        for i in range(files_per_day):
            chunk = drows[i::files_per_day]
            if chunk:
                pq.write_table(pa.Table.from_pylist(chunk), os.path.join(d, "part-%d.parquet" % i))
    return path


@pytest.fixture(scope="session")
def spark():
    s = build_spark("sf-test").newSession()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path):
    return Settings(index_prefix="test", archive_dir=str(tmp_path / "archive"),
                    checkpoint_dir=str(tmp_path / "ckpt"), window="1 day", watermark="1 hour")


@pytest.fixture
def archive(cfg):
    return write_events(cfg.archive_dir, EVENTS)
