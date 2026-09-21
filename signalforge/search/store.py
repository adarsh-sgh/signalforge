"""OpenSearch access behind a small protocol so the pipeline and API can run on an in-memory fake."""
import re
from typing import Dict, Iterable, List, Optional, Protocol

from signalforge.tenancy import doc_id

DOC_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "tenant_id": {"type": "keyword"},
            "entity_id": {"type": "keyword"},
            "day": {"type": "date", "format": "yyyy-MM-dd"},
            "window_start": {"type": "date"},
            "window_end": {"type": "date"},
            "n": {"type": "long"},
            "mean_score": {"type": "double"},
            "min_score": {"type": "double"},
            "max_score": {"type": "double"},
            "stddev_score": {"type": "double"},
            "last_score": {"type": "double"},
            "last_ts": {"type": "date", "format": "epoch_millis"},
            "signal_types": {"type": "keyword"},
            "sources": {"type": "keyword"},
        }
    },
}

_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def index_name(prefix: str, day: str) -> str:
    """Pooled per-day index; `Router.index_for` adds the dedicated-tenant form."""
    if not _DAY_RE.match(day):
        raise ValueError("bad day %r" % day)
    return "%s-%s" % (prefix, day)


def day_of(index: str) -> str:
    """Inverse of index_name; other sinks key on the day only."""
    day = index[-10:]
    if not _DAY_RE.match(day):
        raise ValueError("no day in index name %r" % index)
    return day


def doc_key(d: Dict) -> str:
    return doc_id(d["tenant_id"], d["entity_id"])


class SearchStore(Protocol):
    def ensure_index(self, index: str, shards: Optional[int] = None) -> None: ...
    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int: ...
    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]: ...
    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]: ...
    def count(self, index: str) -> int: ...
    def refresh(self, index: str) -> None: ...
    # lifecycle
    def list_indices(self, pattern: str) -> List[str]: ...
    def delete_index(self, index: str) -> None: ...
    def alias_indices(self, alias: str) -> List[str]: ...
    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> None: ...


class InMemoryStore:
    """Dict-backed fake with the same semantics the API and sink rely on (upsert by _id, routing, aliases)."""

    def __init__(self) -> None:
        self.indices: Dict[str, Dict[str, Dict]] = {}
        self.routing: Dict[str, Dict[str, Optional[str]]] = {}  # index -> _id -> routing value used
        self.shards: Dict[str, int] = {}
        self.aliases: Dict[str, List[str]] = {}

    def ensure_index(self, index: str, shards: Optional[int] = None) -> None:
        if index not in self.indices:
            self.indices[index], self.routing[index] = {}, {}
            self.shards[index] = shards or DOC_MAPPING["settings"]["number_of_shards"]

    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int:
        self.ensure_index(index)
        n = 0
        for d in docs:
            self.indices[index][doc_key(d)] = dict(d)
            self.routing[index][doc_key(d)] = routing
            n += 1
        return n

    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]:
        # a GET routed to the wrong shard misses on real OpenSearch, so the fake misses too
        if index in self.routing and doc_id in self.routing[index] and self.routing[index][doc_id] != routing:
            return None
        return self.indices.get(index, {}).get(doc_id)

    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]:
        return [d for d in (self.get(i, doc_id, routing) for i in indices) if d is not None]

    def count(self, index: str) -> int:
        return len(self.indices.get(index, {}))

    def refresh(self, index: str) -> None:
        pass

    def list_indices(self, pattern: str) -> List[str]:
        rx = re.compile("^" + re.escape(pattern).replace("\\*", ".*") + "$")
        return sorted(i for i in self.indices if rx.match(i))

    def delete_index(self, index: str) -> None:
        for d in (self.indices, self.routing, self.shards):
            d.pop(index, None)
        for members in self.aliases.values():
            if index in members:
                members.remove(index)

    def alias_indices(self, alias: str) -> List[str]:
        return sorted(self.aliases.get(alias, []))

    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> None:
        members = self.aliases.setdefault(alias, [])
        for i in remove:
            if i in members:
                members.remove(i)
        for i in add:
            if i not in self.indices:
                raise KeyError(i)
            if i not in members:
                members.append(i)


class OpenSearchStore:
    def __init__(self, url: str, alias: Optional[str] = None) -> None:
        from opensearchpy import OpenSearch, helpers

        self._helpers = helpers
        self.client = OpenSearch(hosts=[url], timeout=30)
        self.alias = alias

    def ensure_index(self, index: str, shards: Optional[int] = None) -> None:
        if not self.client.indices.exists(index=index):
            body = {"settings": dict(DOC_MAPPING["settings"]), "mappings": DOC_MAPPING["mappings"]}
            if shards:
                body["settings"]["number_of_shards"] = shards
            if self.alias:
                body["aliases"] = {self.alias: {}}
            self.client.indices.create(index=index, body=body, ignore=400)

    def bulk_upsert(self, index: str, docs: Iterable[Dict], routing: Optional[str] = None) -> int:
        self.ensure_index(index)
        extra = {"routing": routing} if routing else {}
        actions = (dict({"_op_type": "index", "_index": index, "_id": doc_key(d), "_source": d}, **extra)
                   for d in docs)
        ok, _ = self._helpers.bulk(self.client, actions, chunk_size=1000, request_timeout=60)
        return ok

    def get(self, index: str, doc_id: str, routing: Optional[str] = None) -> Optional[Dict]:
        from opensearchpy.exceptions import NotFoundError

        try:
            return self.client.get(index=index, id=doc_id, routing=routing)["_source"]
        except NotFoundError:
            return None

    def mget(self, indices: List[str], doc_id: str, routing: Optional[str] = None) -> List[Dict]:
        existing = [i for i in indices if self.client.indices.exists(index=i)]
        if not existing:
            return []
        extra = {"routing": routing} if routing else {}
        res = self.client.mget(body={"docs": [dict({"_index": i, "_id": doc_id}, **extra) for i in existing]})
        return [d["_source"] for d in res["docs"] if d.get("found")]

    def count(self, index: str) -> int:
        if not self.client.indices.exists(index=index):
            return 0
        return self.client.count(index=index)["count"]

    def refresh(self, index: str) -> None:
        self.client.indices.refresh(index=index)

    def list_indices(self, pattern: str) -> List[str]:
        return sorted(self.client.indices.get(index=pattern, ignore_unavailable=True).keys())

    def delete_index(self, index: str) -> None:
        self.client.indices.delete(index=index, ignore=404)

    def alias_indices(self, alias: str) -> List[str]:
        if not self.client.indices.exists_alias(name=alias):
            return []
        return sorted(self.client.indices.get_alias(name=alias).keys())

    def update_alias(self, alias: str, add: List[str], remove: List[str]) -> None:
        actions = ([{"remove": {"index": i, "alias": alias}} for i in remove]
                   + [{"add": {"index": i, "alias": alias}} for i in add])
        if actions:  # one atomic swap, readers never see an empty alias
            self.client.indices.update_aliases(body={"actions": actions})


def open_store(url: str, alias: Optional[str] = None) -> OpenSearchStore:
    return OpenSearchStore(url, alias=alias)
