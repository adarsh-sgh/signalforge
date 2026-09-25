"""Admission service in front of Trino.

Clients submit statements here instead of to the coordinator; the guard decides, the allowed ones
are forwarded, and every decision plus the outcome of every admitted query is audited. Refusals are
403 with the rule that fired, throttles are 429 with `Retry-After`, so a client can tell "never" from
"not right now". The concurrency slot is released in a `finally`, so a query that errors out inside
Trino does not leak the user's capacity.
"""
import time
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from signalforge.config import Settings, settings
from signalforge.metrics import HTTP_LATENCY, HTTP_REQUESTS, TRINO_QUERY_SECONDS, TRINO_SCANNED
from signalforge.trino.audit import AuditLog, Outcome, open_audit
from signalforge.trino.client import TrinoClient
from signalforge.trino.guard import REJECT, THROTTLE, Guard, Policy


class QueryRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=100_000)
    user: Optional[str] = None


def load_policy(cfg: Settings = settings) -> Policy:
    if cfg.trino_policy_file:
        with open(cfg.trino_policy_file) as fh:
            return Policy.from_json(fh.read())
    return Policy(tables=Policy.default(cfg.lake_table).tables,
                  max_scan_bytes=cfg.trino_max_scan_bytes,
                  max_concurrent_per_user=cfg.trino_max_concurrent_per_user)


def create_app(client, policy: Optional[Policy] = None, audit: Optional[AuditLog] = None,
               cfg: Settings = settings) -> FastAPI:
    app = FastAPI(title="signalforge trino guard", version="0.1")
    audit = audit if audit is not None else open_audit(cfg.trino_audit_file)
    guard = Guard(policy or load_policy(cfg), estimator=client.estimate_scan_bytes, audit=audit)
    app.state.guard, app.state.audit, app.state.client = guard, audit, client

    @app.middleware("http")
    async def observe(request: Request, call_next):
        t0 = time.time()
        resp = await call_next(request)
        path = request.scope.get("route").path if request.scope.get("route") else request.url.path
        HTTP_REQUESTS.labels(path=path, status=resp.status_code).inc()
        HTTP_LATENCY.labels(path=path).observe(time.time() - t0)
        return resp

    def who(body_user: Optional[str], header_user: Optional[str]) -> str:
        return body_user or header_user or "anonymous"

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "trino": client.health()}

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/policy")
    def get_policy():
        p = guard.policy
        return {"max_scan_bytes": p.max_scan_bytes, "max_concurrent_per_user": p.max_concurrent_per_user,
                "require_limit_on_star": p.require_limit_on_star, "allowed_kinds": list(p.allowed_kinds),
                "tables": {name: {"partition_columns": list(t.partition_columns),
                                  "max_scan_bytes": t.max_scan_bytes} for name, t in p.tables.items()}}

    @app.get("/v1/decisions")
    def decisions(limit: int = Query(50, ge=1, le=1000)):
        return {"decisions": audit.tail(limit)}

    @app.get("/v1/summary")
    def summary():
        return audit.summary()

    @app.post("/v1/queries")
    def submit(req: QueryRequest, x_trino_user: Optional[str] = Header(None)):
        user = who(req.user, x_trino_user)
        decision = guard.admit(user, req.sql)
        if decision.verdict == REJECT:
            raise HTTPException(403, detail=decision.to_dict())
        if decision.verdict == THROTTLE:
            raise HTTPException(429, detail=decision.to_dict(),
                                headers={"Retry-After": str(decision.retry_after_seconds)})
        t0 = time.time()
        try:
            result = client.run(req.sql, user=user)
        except Exception as e:
            audit.record_outcome(Outcome(decision.query_hash, "", "FAILED", error=str(e)[:500]))
            raise HTTPException(502, detail={"error": str(e)[:500], "decision": decision.to_dict()})
        finally:
            guard.release(user)
            TRINO_QUERY_SECONDS.observe(time.time() - t0)
        audit.record_outcome(Outcome(decision.query_hash, result.query_id, result.state,
                                     processed_bytes=result.processed_bytes,
                                     processed_rows=result.processed_rows, wall_ms=result.wall_ms))
        if result.processed_bytes:
            TRINO_SCANNED.inc(result.processed_bytes)
        return {"decision": decision.to_dict(), "columns": list(result.columns),
                "rows": [list(r) for r in result.rows],
                "stats": {"query_id": result.query_id, "processed_bytes": result.processed_bytes,
                          "processed_rows": result.processed_rows, "wall_ms": result.wall_ms}}

    return app


def main() -> None:
    import uvicorn

    uvicorn.run(create_app(TrinoClient(settings)), host="0.0.0.0", port=settings.trino_guard_port)


if __name__ == "__main__":
    main()
