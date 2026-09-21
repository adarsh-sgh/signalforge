"""Read API over the per-day rollups (SF_SINK picks the store, REDIS_URL adds a lookup cache).
Point lookups only; no scoring logic lives here. `/entities/...` is shorthand for the default tenant."""
import datetime as dt
import time
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from signalforge.config import settings
from signalforge.metrics import HTTP_LATENCY, HTTP_REQUESTS
from signalforge.search.store import SearchStore
from signalforge.sinks import open_sink
from signalforge.tenancy import DEFAULT_TENANT, Router, doc_id

_DAY = r"^\d{4}-\d{2}-\d{2}$"
_TENANT = Path(pattern=r"^[a-z0-9][a-z0-9_.-]{0,62}$")


def _today() -> str:
    return dt.date.today().isoformat()


def _days_back(end: str, n: int):
    d = dt.date.fromisoformat(end)
    return [(d - dt.timedelta(days=i)).isoformat() for i in range(n)]


def create_app(store: SearchStore, prefix: str = settings.index_prefix, router: Optional[Router] = None) -> FastAPI:
    app = FastAPI(title="signalforge", version="0.2")
    router = router or Router(prefix)

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

    def summary(tenant: str, entity_id: str, day: Optional[str], s: SearchStore):
        day = day or _today()
        doc = s.get(router.index_for(tenant, day), doc_id(tenant, entity_id), router.routing_for(tenant, entity_id))
        if doc is None:
            raise HTTPException(404, "no signals for %s/%s on %s" % (tenant, entity_id, day))
        return doc

    def history(tenant: str, entity_id: str, days: int, end: Optional[str], s: SearchStore):
        indices = [router.index_for(tenant, d) for d in _days_back(end or _today(), days)]
        docs = s.mget(indices, doc_id(tenant, entity_id), router.routing_for(tenant, entity_id))
        docs = sorted(docs, key=lambda d: d["day"])
        total = sum(d["n"] for d in docs)
        mean = round(sum(d["mean_score"] * d["n"] for d in docs) / total, 4) if total else None
        return {"tenant_id": tenant, "entity_id": entity_id, "days": len(docs), "events": total,
                "mean_score": mean, "daily": docs}

    @app.get("/tenants/{tenant}/entities/{entity_id}")
    def tenant_entity_summary(entity_id: str, tenant: str = _TENANT, day: Optional[str] = Query(None, pattern=_DAY),
                              s: SearchStore = Depends(get_store)):
        return summary(tenant, entity_id, day, s)

    @app.get("/tenants/{tenant}/entities/{entity_id}/history")
    def tenant_entity_history(entity_id: str, tenant: str = _TENANT, days: int = Query(7, ge=1, le=90),
                              end: Optional[str] = Query(None, pattern=_DAY), s: SearchStore = Depends(get_store)):
        return history(tenant, entity_id, days, end, s)

    @app.get("/entities/{entity_id}")
    def entity_summary(entity_id: str, day: Optional[str] = Query(None, pattern=_DAY),
                       s: SearchStore = Depends(get_store)):
        return summary(DEFAULT_TENANT, entity_id, day, s)

    @app.get("/entities/{entity_id}/history")
    def entity_history(entity_id: str, days: int = Query(7, ge=1, le=90),
                       end: Optional[str] = Query(None, pattern=_DAY), s: SearchStore = Depends(get_store)):
        return history(DEFAULT_TENANT, entity_id, days, end, s)

    return app


def main() -> None:
    import uvicorn

    uvicorn.run(create_app(open_sink(settings), router=Router.from_settings(settings)),
                host="0.0.0.0", port=settings.api_port)


if __name__ == "__main__":
    main()
