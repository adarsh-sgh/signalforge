import json
import subprocess
import sys

import pytest

from signalforge.capacity import Assumptions, Workload, measure_doc_bytes, plan
from signalforge.pipeline.job import run_batch
from signalforge.search.store import InMemoryStore


def test_plan_hand_checked_and_bottleneck_switches():
    w = Workload(events_per_sec=20_000, entities_per_day=2_000_000, retention_days=30, replicas=1, doc_bytes=400)
    p = plan(w, "r6g.large.search")
    assert p.primary_gib_per_day == round(2e6 * 400 * 1.3 / 2 ** 30, 3) == 0.969
    assert p.upserts_per_sec == 20_000  # events/s < entities per micro-batch
    assert p.shards_per_day == 1 and p.total_primary_shards == 31 and p.total_shards == 62
    assert p.storage_gib == round(0.9686 * 31 * 2 / 0.8, 0) == 75.0 or abs(p.storage_gib - 75.1) < 0.2
    assert (p.nodes_by_storage, p.nodes_by_shards, p.nodes_by_ingest) == (1, 1, 20)  # 40k upserts/s / (2 vCPU x 1000)
    assert p.data_nodes == 20 and p.bottleneck == "ingest" and p.shards_per_node == 3.1
    assert p.monthly_instance_usd == round(20 * 0.167 * 730, 2) and p.monthly_storage_usd == round(20 * 512 * 0.122, 2)
    assert p.monthly_usd == round(p.monthly_instance_usd + p.monthly_storage_usd, 2)

    # bigger nodes take fewer of them; a price override flows into the bill
    big = plan(w, "r6g.2xlarge.search", hourly=1.0)
    assert big.nodes_by_ingest == 5 and big.data_nodes == 5 and big.monthly_instance_usd == 5 * 730.0

    # long retention of large docs at a low event rate is storage-bound
    lo = Workload(200, 5_000_000, retention_days=365, replicas=1, doc_bytes=2000)
    s = plan(lo, "r6g.large.search")
    assert s.bottleneck == "storage" and (s.nodes_by_storage, s.nodes_by_shards, s.nodes_by_ingest) == (22, 4, 1)
    assert plan(Workload(200, 5_000_000, retention_days=365, replicas=3, doc_bytes=2000)).storage_gib == pytest.approx(
        2 * s.storage_gib, rel=1e-6)
    # giving 40 tenants their own daily index adds 40 shards a day; over a year that outgrows storage as the limit
    d = plan(Workload(200, 5_000_000, retention_days=365, replicas=1, doc_bytes=2000, dedicated_tenants=40,
                      dedicated_share=0.5), "r6g.large.search")
    assert d.shards_per_day == 41 and d.total_shards == 41 * 366 * 2 and d.bottleneck == "shards" and d.data_nodes == 151

    # thousands of tiny dedicated tenants on a small heap: shard-bound at an absurd node count, hence pooling
    t = plan(Workload(100, 100_000, retention_days=90, replicas=1, dedicated_tenants=2000, dedicated_share=0.9),
             "m6g.large.search", Assumptions(ebs_gib_per_node=4096))
    assert t.bottleneck == "shards" and t.total_shards == 2001 * 91 * 2 and t.nodes_by_shards == 3642
    assert plan(Workload(100, 100, replicas=0)).data_nodes == 1 and plan(Workload(100, 100)).bottleneck == "replica floor"
    with pytest.raises(ValueError):
        plan(Workload(1, 0))


def test_measured_doc_size_feeds_cli(spark, cfg, archive):
    store = InMemoryStore()
    run_batch(spark, archive, store, cfg)
    size = measure_doc_bytes(d for docs in store.indices.values() for d in docs.values())
    assert 250 < size < 450 and measure_doc_bytes([]) == 0.0
    out = subprocess.run([sys.executable, "-m", "signalforge.capacity", "--events-per-sec", "5000",
                          "--entities-per-day", "500000", "--doc-bytes", str(size), "--replicas", "0"],
                         capture_output=True, text=True, check=True, env={"PYTHONPATH": ".", "PATH": ""}).stdout
    p = json.loads(out)
    assert p["docs_per_day"] == 500_000 and p["data_nodes"] == 3 and p["bottleneck"] == "ingest"
    assert p["primary_gib_per_day"] == round(500_000 * size * 1.3 / 2 ** 30, 3)
