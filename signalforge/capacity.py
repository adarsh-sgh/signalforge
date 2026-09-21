"""Capacity and cost model for the OpenSearch sink.

From an event rate, distinct entities per day, retention and replica count it derives documents,
index size, shards per daily index, total shards, the data nodes needed (storage-, shard- or
ingest-bound) and a monthly bill. Every constant is an explicit, overridable assumption: doc size
comes from `measure_doc_bytes` over real documents (the bench prints it), the rest from OpenSearch
sizing guidance (10-50 GiB per shard, <= 25 shards per GiB of heap) and list prices.

    python -m signalforge.capacity --events-per-sec 20000 --entities-per-day 2000000 --retention-days 30
"""
import argparse
import json
import math
from dataclasses import asdict, dataclass
from typing import Dict, Iterable

GIB = 2 ** 30
HOURS_PER_MONTH = 730

# vCPU, memory GiB, on-demand USD/hour. Approximate us-east-1 list prices; override with --hourly.
NODE_TYPES: Dict[str, tuple] = {
    "m6g.large.search": (2, 8, 0.128),
    "m6g.xlarge.search": (4, 16, 0.256),
    "r6g.large.search": (2, 16, 0.167),
    "r6g.xlarge.search": (4, 32, 0.335),
    "r6g.2xlarge.search": (8, 64, 0.669),
}
EBS_USD_PER_GIB_MONTH = 0.122  # gp3, approximate


@dataclass
class Workload:
    events_per_sec: float
    entities_per_day: int          # distinct (tenant, entity) per day = documents per day
    retention_days: int = 30
    replicas: int = 1
    doc_bytes: float = 400.0       # JSON _source size; measure with measure_doc_bytes
    dedicated_tenants: int = 0     # each adds one index (and its shards) per day
    dedicated_share: float = 0.0   # fraction of documents that belong to dedicated tenants
    trigger_seconds: float = 10.0  # streaming micro-batch interval


@dataclass
class Assumptions:
    index_overhead: float = 1.3          # on-disk bytes per byte of _source (doc values + inverted index)
    target_shard_gib: float = 30.0       # OpenSearch guidance: 10-50 GiB per shard
    max_shards_per_gib_heap: float = 25  # AWS OpenSearch guidance
    heap_fraction: float = 0.5           # JVM heap = half of memory, capped at 32 GiB
    disk_headroom: float = 0.8           # stay under the 85% high watermark with room for merges
    docs_per_sec_per_vcpu: float = 1000  # sustained bulk upserts per vCPU; measure on your cluster
    ebs_gib_per_node: float = 512


@dataclass
class Plan:
    docs_per_day: int
    upserts_per_sec: float
    primary_gib_per_day: float
    shards_per_day: int            # primaries across the pooled index + dedicated indices
    total_primary_shards: int
    total_shards: int
    storage_gib: float             # provisioned, after replicas and headroom
    nodes_by_storage: int
    nodes_by_shards: int
    nodes_by_ingest: int
    data_nodes: int
    bottleneck: str
    shards_per_node: float
    node_type: str
    monthly_instance_usd: float
    monthly_storage_usd: float
    monthly_usd: float


def measure_doc_bytes(docs: Iterable[Dict]) -> float:
    """Mean JSON size of the documents actually produced; 0 if none."""
    n = total = 0
    for d in docs:
        total += len(json.dumps(d, separators=(",", ":")))
        n += 1
    return total / n if n else 0.0


def shards_for_gib(gib: float, target_shard_gib: float) -> int:
    return max(1, math.ceil(gib / target_shard_gib))


def plan(w: Workload, node_type: str = "r6g.large.search", a: Assumptions = Assumptions(),
         hourly: float = None) -> Plan:
    if w.replicas < 0 or w.retention_days < 1 or w.entities_per_day < 1:
        raise ValueError("replicas >= 0, retention_days >= 1, entities_per_day >= 1")
    vcpu, mem_gib, list_price = NODE_TYPES[node_type]
    hourly = list_price if hourly is None else hourly

    days_held = w.retention_days + 1  # tomorrow's index is pre-created by the lifecycle step
    docs = w.entities_per_day
    primary_gib = docs * w.doc_bytes * a.index_overhead / GIB
    # every micro-batch re-upserts each touched entity, so writes are bounded by both rates
    upserts = min(w.events_per_sec, docs / w.trigger_seconds)

    pooled_gib = primary_gib * (1 - w.dedicated_share)
    ded_gib = primary_gib * w.dedicated_share / w.dedicated_tenants if w.dedicated_tenants else 0.0
    shards_per_day = shards_for_gib(pooled_gib, a.target_shard_gib)
    shards_per_day += w.dedicated_tenants * shards_for_gib(ded_gib, a.target_shard_gib)
    total_primary = shards_per_day * days_held
    total_shards = total_primary * (1 + w.replicas)

    storage = primary_gib * days_held * (1 + w.replicas) / a.disk_headroom
    heap_gib = min(32.0, mem_gib * a.heap_fraction)
    by_storage = math.ceil(storage / a.ebs_gib_per_node)
    by_shards = math.ceil(total_shards / (heap_gib * a.max_shards_per_gib_heap))
    by_ingest = math.ceil(upserts * (1 + w.replicas) / (vcpu * a.docs_per_sec_per_vcpu))
    floor = 1 + w.replicas  # a replica needs a second node
    nodes = max(by_storage, by_shards, by_ingest, floor)
    bottleneck = {by_storage: "storage", by_shards: "shards", by_ingest: "ingest"}.get(nodes, "replica floor")

    instance_usd = nodes * hourly * HOURS_PER_MONTH
    storage_usd = nodes * a.ebs_gib_per_node * EBS_USD_PER_GIB_MONTH
    return Plan(docs, round(upserts, 1), round(primary_gib, 3), shards_per_day, total_primary, total_shards,
                round(storage, 1), by_storage, by_shards, by_ingest, nodes, bottleneck,
                round(total_shards / nodes, 1), node_type, round(instance_usd, 2), round(storage_usd, 2),
                round(instance_usd + storage_usd, 2))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--events-per-sec", type=float, required=True)
    ap.add_argument("--entities-per-day", type=int, required=True)
    ap.add_argument("--retention-days", type=int, default=30)
    ap.add_argument("--replicas", type=int, default=1)
    ap.add_argument("--doc-bytes", type=float, default=400.0)
    ap.add_argument("--dedicated-tenants", type=int, default=0)
    ap.add_argument("--dedicated-share", type=float, default=0.0)
    ap.add_argument("--node-type", choices=sorted(NODE_TYPES), default="r6g.large.search")
    ap.add_argument("--hourly", type=float, default=None, help="override the node's USD/hour")
    ap.add_argument("--target-shard-gib", type=float, default=Assumptions.target_shard_gib)
    ap.add_argument("--docs-per-sec-per-vcpu", type=float, default=Assumptions.docs_per_sec_per_vcpu)
    ap.add_argument("--ebs-gib-per-node", type=float, default=Assumptions.ebs_gib_per_node)
    args = ap.parse_args()
    w = Workload(args.events_per_sec, args.entities_per_day, args.retention_days, args.replicas, args.doc_bytes,
                 args.dedicated_tenants, args.dedicated_share)
    a = Assumptions(target_shard_gib=args.target_shard_gib, docs_per_sec_per_vcpu=args.docs_per_sec_per_vcpu,
                    ebs_gib_per_node=args.ebs_gib_per_node)
    print(json.dumps(asdict(plan(w, args.node_type, a, args.hourly)), indent=2))


if __name__ == "__main__":
    main()
