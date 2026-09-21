"""Tenant-aware index routing and per-tenant admission quotas.

Small tenants share one pooled index per day (`prefix-YYYY-MM-DD`) and are pinned to a shard
with a `_routing` value, so a tenant's reads and writes touch one shard instead of fanning out.
Tenants listed in `dedicated` get their own daily index (`prefix-<tenant>-YYYY-MM-DD`) with the
default hash-by-id spread; that is the escape hatch for a tenant big enough to make a pooled
shard hot. A pooled tenant can also be split over `partitions[tenant]` routing values.
"""
import re
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from signalforge.config import Settings
from signalforge.metrics import QUOTA_DROPPED

DEFAULT_TENANT = "default"
_DAY = r"\d{4}-\d{2}-\d{2}"
_TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")


def doc_id(tenant: str, entity_id: str) -> str:
    """`_id` in a pooled index must not collide across tenants."""
    return "%s:%s" % (tenant, entity_id)


def split_doc_id(doc_id_: str) -> Tuple[str, str]:
    tenant, _, entity = doc_id_.partition(":")
    return tenant, entity


def check_tenant(tenant: str) -> str:
    if not _TENANT_RE.match(tenant):
        raise ValueError("bad tenant %r" % tenant)
    return tenant


def _parse_map(spec: str) -> Dict[str, int]:
    """'acme=4,beta=2' -> {'acme': 4, 'beta': 2}"""
    out = {}
    for part in filter(None, (p.strip() for p in spec.split(","))):
        k, _, v = part.partition("=")
        out[check_tenant(k.strip())] = int(v)
    return out


@dataclass(frozen=True)
class Router:
    prefix: str
    dedicated: FrozenSet[str] = frozenset()
    partitions: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_settings(cls, cfg: Settings) -> "Router":
        dedicated = frozenset(check_tenant(t.strip()) for t in cfg.dedicated_tenants.split(",") if t.strip())
        return cls(cfg.index_prefix, dedicated, _parse_map(cfg.routing_partitions))

    def is_dedicated(self, tenant: str) -> bool:
        return tenant in self.dedicated

    def index_for(self, tenant: str, day: str) -> str:
        if not re.match("^%s$" % _DAY, day):
            raise ValueError("bad day %r" % day)
        if self.is_dedicated(tenant):
            return "%s-%s-%s" % (self.prefix, tenant, day)
        return "%s-%s" % (self.prefix, day)

    def routing_for(self, tenant: str, entity_id: str) -> Optional[str]:
        """Pooled: pin the tenant to one shard (or `partitions[tenant]` shards). Dedicated: hash by id."""
        if self.is_dedicated(tenant):
            return None
        n = self.partitions.get(tenant, 1)
        if n <= 1:
            return tenant
        return "%s#%d" % (tenant, zlib.crc32(entity_id.encode()) % n)

    def alias_for(self, tenant: str) -> str:
        """Read alias spanning the retention window; one per pooled index family or dedicated tenant."""
        return "%s-%s" % (self.prefix, tenant) if self.is_dedicated(tenant) else self.prefix

    def families(self) -> List[Tuple[str, Optional[str]]]:
        """(alias, tenant-or-None) for every index family this router writes."""
        return [(self.prefix, None)] + [(self.alias_for(t), t) for t in sorted(self.dedicated)]

    def day_indices(self, day: str) -> List[str]:
        return [self.index_for(t or DEFAULT_TENANT, day) for _, t in self.families()]

    def parse_index(self, index: str) -> Optional[Tuple[Optional[str], str]]:
        """Inverse of index_for: (tenant-or-None, day), or None if the name is not ours."""
        m = re.match(r"^%s(?:-(?P<tenant>.+?))?-(?P<day>%s)$" % (re.escape(self.prefix), _DAY), index)
        if not m:
            return None
        tenant = m.group("tenant")
        if tenant is not None and tenant not in self.dedicated:
            return None
        return tenant, m.group("day")


class Quota:
    """Cap on distinct entities a tenant may roll up per day; docs past the cap are dropped and counted.

    Entities already admitted always pass, since every micro-batch re-emits the running rollup for a
    touched entity. The admitted sets live in the driver process: exact within a run, reset on restart
    (a restart replays from the checkpoint, so the sets refill from the same documents).
    """

    def __init__(self, limits: Dict[str, int], default: int = 0, keep_days: int = 3) -> None:
        self.limits, self.default, self.keep_days = dict(limits), default, keep_days
        self.admitted: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

    @classmethod
    def from_settings(cls, cfg: Settings) -> Optional["Quota"]:
        limits = _parse_map(cfg.tenant_quota)
        return cls(limits, cfg.tenant_quota_default) if limits or cfg.tenant_quota_default else None

    def limit_for(self, tenant: str) -> int:
        return self.limits.get(tenant, self.default)

    def admit(self, tenant: str, day: str, entity_id: str) -> bool:
        limit = self.limit_for(tenant)
        if limit <= 0:
            return True
        seen = self.admitted[(tenant, day)]
        if entity_id in seen:
            return True
        if len(seen) >= limit:
            QUOTA_DROPPED.labels(tenant=tenant).inc()
            return False
        seen.add(entity_id)
        return True

    def forget_before(self, day: str) -> None:
        """Drop tracking for days older than `day` so the sets don't grow forever."""
        for key in [k for k in self.admitted if k[1] < day]:
            del self.admitted[key]
