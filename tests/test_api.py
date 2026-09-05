from fastapi.testclient import TestClient

from signalforge.api.app import create_app
from signalforge.pipeline.job import run_batch
from signalforge.search.store import InMemoryStore
from tests.conftest import D1, D2


def test_summary_history_and_metrics(spark, cfg, archive):
    store = InMemoryStore()
    run_batch(spark, archive, store, cfg)
    c = TestClient(create_app(store, prefix="test"))

    r = c.get("/entities/ent-1", params={"day": D1})
    assert r.status_code == 200 and r.json()["n"] == 3
    assert c.get("/entities/ent-9", params={"day": D1}).status_code == 404
    assert c.get("/entities/ent-1", params={"day": "yesterday"}).status_code == 422

    h = c.get("/entities/ent-1/history", params={"end": D2, "days": 7}).json()
    assert [d["day"] for d in h["daily"]] == [D1, D2]
    assert h["events"] == 4 and h["mean_score"] == 3.5  # (4+2+5+3)/4

    m = c.get("/metrics").text
    assert 'sf_http_requests_total{path="/entities/{entity_id}",status="404"}' in m
    assert "sf_docs_upserted_total" in m
