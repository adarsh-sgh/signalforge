"""OpenSearch access behind a small protocol so the pipeline and API can run on an in-memory fake."""
import re
from typing import Dict, Iterable, List, Optional, Protocol

DOC_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
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
    if not _DAY_RE.match(day):
        raise ValueError("bad day %r" % day)
    return "%s-%s" % (prefix, day)


def day_of(index: str) -> str:
    """Inverse of index_name; other sinks key on the day only."""
    day = index[-10:]
    if not _DAY_RE.match(day):
        raise ValueError("no day in index name %r" % index)
    return day


class SearchStore(Protocol):
    def ensure_index(self, index: str) -> None: ...
    def bulk_upsert(self, index: str, docs: Iterable[Dict]) -> int: ...
    def get(self, index: str, doc_id: str) -> Optional[Dict]: ...
    def mget(self, indices: List[str], doc_id: str) -> List[Dict]: ...
    def count(self, index: str) -> int: ...
    def refresh(self, index: str) -> None: ...


class InMemoryStore:
    """Dict-backed fake with the same semantics the API and sink rely on (upsert by _id)."""

    def __init__(self) -> None:
        self.indices: Dict[str, Dict[str, Dict]] = {}

    def ensure_index(self, index: str) -> None:
        self.indices.setdefault(index, {})

    def bulk_upsert(self, index: str, docs: Iterable[Dict]) -> int:
        self.ensure_index(index)
        n = 0
        for d in docs:
            self.indices[index][d["entity_id"]] = dict(d)
            n += 1
        return n

    def get(self, index: str, doc_id: str) -> Optional[Dict]:
        return self.indices.get(index, {}).get(doc_id)

    def mget(self, indices: List[str], doc_id: str) -> List[Dict]:
        return [d for d in (self.get(i, doc_id) for i in indices) if d is not None]

    def count(self, index: str) -> int:
        return len(self.indices.get(index, {}))

    def refresh(self, index: str) -> None:
        pass


class OpenSearchStore:
    def __init__(self, url: str, alias: Optional[str] = None) -> None:
        from opensearchpy import OpenSearch, helpers

        self._helpers = helpers
        self.client = OpenSearch(hosts=[url], timeout=30)
        self.alias = alias

    def ensure_index(self, index: str) -> None:
        if not self.client.indices.exists(index=index):
            body = dict(DOC_MAPPING)
            if self.alias:
                body["aliases"] = {self.alias: {}}
            self.client.indices.create(index=index, body=body, ignore=400)

    def bulk_upsert(self, index: str, docs: Iterable[Dict]) -> int:
        self.ensure_index(index)
        actions = ({"_op_type": "index", "_index": index, "_id": d["entity_id"], "_source": d}
                   for d in docs)
        ok, _ = self._helpers.bulk(self.client, actions, chunk_size=1000, request_timeout=60)
        return ok

    def get(self, index: str, doc_id: str) -> Optional[Dict]:
        from opensearchpy.exceptions import NotFoundError

        try:
            return self.client.get(index=index, id=doc_id)["_source"]
        except NotFoundError:
            return None

    def mget(self, indices: List[str], doc_id: str) -> List[Dict]:
        existing = [i for i in indices if self.client.indices.exists(index=i)]
        if not existing:
            return []
        res = self.client.mget(body={"docs": [{"_index": i, "_id": doc_id} for i in existing]})
        return [d["_source"] for d in res["docs"] if d.get("found")]

    def count(self, index: str) -> int:
        if not self.client.indices.exists(index=index):
            return 0
        return self.client.count(index=index)["count"]

    def refresh(self, index: str) -> None:
        self.client.indices.refresh(index=index)


def open_store(url: str, alias: str) -> OpenSearchStore:
    return OpenSearchStore(url, alias=alias)
