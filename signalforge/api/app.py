"""Read API over the per-day indices. Point lookups only; no scoring logic lives here."""
import datetime as dt
import time
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from signalforge.config import settings
from signalforge.metrics import HTTP_LATENCY, HTTP_REQUESTS
from signalforge.search.store import SearchStore, index_name, open_store


def _today() -> str:
    return dt.date.today().isoformat()


def _days_back(end: str, n: int):
    d = dt.date.fromisoformat(end)
    return [(d - dt.timedelta(days=i)).isoformat() for i in range(n)]


def create_app(store: SearchStore, prefix: str = settings.index_prefix) -> FastAPI:
    app = FastAPI(title="signalforge", version="0.1")

    def get_store() -> SearchStore:
        return store

    @app.middleware("http")
    async def observe(request: Request, call_next):
        t0 = time.time()
        resp = await call_next(request)
        path = request.scope.get("route").path if request.scope.get("route") else request.url.path
        HTTP_REQUESTS.labels(path=path, status=resp.status_code).inc()
        HTTP_LATENCY.labels(path=path).observe(time.time() - t0)
        return resp

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/entities/{entity_id}")
    def entity_summary(entity_id: str, day: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
                       s: SearchStore = Depends(get_store)):
        day = day or _today()
        doc = s.get(index_name(prefix, day), entity_id)
        if doc is None:
            raise HTTPException(404, "no signals for %s on %s" % (entity_id, day))
        return doc

    @app.get("/entities/{entity_id}/history")
    def entity_history(entity_id: str, days: int = Query(7, ge=1, le=90),
                       end: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
                       s: SearchStore = Depends(get_store)):
        indices = [index_name(prefix, d) for d in _days_back(end or _today(), days)]
        docs = sorted(s.mget(indices, entity_id), key=lambda d: d["day"])
        total = sum(d["n"] for d in docs)
        mean = round(sum(d["mean_score"] * d["n"] for d in docs) / total, 4) if total else None
        return {"entity_id": entity_id, "days": len(docs), "events": total, "mean_score": mean, "daily": docs}

    return app


def main() -> None:
    import uvicorn

    uvicorn.run(create_app(open_store(settings.opensearch_url, settings.index_prefix)),
                host="0.0.0.0", port=settings.api_port)


if __name__ == "__main__":
    main()
