"""Thin FastAPI routes for the causal SPXW replay catalog."""

from __future__ import annotations

import hmac
import logging
import os
from collections.abc import Mapping
from datetime import date, datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import ET
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import ReplaySourceError
from spx_spark.surface_replay_service import (
    DEFAULT_FRAME_MINUTES,
    SERVICE_SCHEMA_VERSION,
    ReplayBusyError,
    ReplayCacheError,
    ReplayCatalog,
    ReplayRequestError,
)
from spx_spark.surface_replay_session import SessionSurfaceSelector
from spx_spark.surface_replay_trend import TrendSelector


LOGGER = logging.getLogger(__name__)
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": "default-src 'none'",
}


def create_app(catalog: ReplayCatalog) -> FastAPI:
    app = FastAPI(
        title="SPX Spark Core API", docs_url=None, redoc_url=None, openapi_url=None
    )

    @app.middleware("http")
    async def bounded_secure_response(request: Request, call_next):
        target = request.scope.get("raw_path", b"") + b"?" + request.scope.get("query_string", b"")
        if len(target) > 2048:
            response: Response = JSONResponse(
                {"error": "invalid_request_target"}, status_code=400
            )
        else:
            response = await call_next(request)
        response.headers.update(SECURITY_HEADERS)
        return response
    @app.exception_handler(ReplayRequestError)
    async def request_error(_request: Request, exc: ReplayRequestError):
        return JSONResponse({"error": exc.code}, status_code=exc.status.value)
    @app.exception_handler(ReplaySourceError)
    async def source_error(_request: Request, exc: ReplaySourceError):
        LOGGER.warning("known-clock replay frame rejected: %s", type(exc).__name__)
        return JSONResponse({"error": "replay_frame_source_rejected"}, status_code=422)
    @app.exception_handler(ReplayCacheError)
    async def cache_error(_request: Request, exc: ReplayCacheError):
        LOGGER.error("replay cache integrity failure: %s", type(exc).__name__)
        return JSONResponse({"error": "replay_cache_integrity_failure"}, status_code=500)
    @app.exception_handler(ReplayBusyError)
    async def busy_error(_request: Request, _exc: ReplayBusyError):
        return JSONResponse({"error": "replay_generation_busy"}, status_code=503,
                            headers={"Retry-After": "2"})
    @app.exception_handler(StarletteHTTPException)
    async def protocol_error(_request: Request, exc: StarletteHTTPException):
        code = "route_not_found" if exc.status_code == 404 else "method_not_allowed"
        headers = {"Allow": "GET, HEAD"} if exc.status_code == 405 else None
        return JSONResponse({"error": code}, status_code=exc.status_code, headers=headers)
    @app.exception_handler(Exception)
    async def internal_error(_request: Request, _exc: Exception):
        LOGGER.exception("unexpected replay API failure")
        return JSONResponse({"error": "internal_error"}, status_code=500)
    @app.api_route("/healthz", methods=["GET", "HEAD"])
    def health() -> Response:
        available = catalog.data_root.is_dir()
        payload = {"status": "ok" if available else "unavailable", "service":
                   "spxw-surface-replay", "schema_version": SERVICE_SCHEMA_VERSION}
        return JSONResponse(payload, status_code=200 if available else 503,
                            headers={"Cache-Control": "no-store"})
    @app.api_route("/api/v1/replay/sessions", methods=["GET", "HEAD"])
    def sessions(request: Request) -> Response:
        _query(request)
        return JSONResponse(catalog.sessions_payload(), headers={"Cache-Control": "private, max-age=30"})
    @app.api_route("/api/v1/replay/sessions/{session}/timeline", methods=["GET", "HEAD"])
    def timeline(session: str, request: Request) -> Response:
        values = _query(request, allowed={"step_minutes"})
        if "step_minutes" in values and _integer(values["step_minutes"], "invalid_step_minutes") != catalog.frame_minutes:
            raise ReplayRequestError("unsupported_step_minutes")
        return JSONResponse(catalog.timeline_payload(_date(session)), headers={
            "Cache-Control": "private, max-age=30"})
    @app.api_route("/api/v1/replay/sessions/{session}/trend", methods=["GET", "HEAD"])
    def trend(session: str, request: Request) -> Response:
        values = _query(request, required={"role", "weighting", "metric"})
        try:
            selector = TrendSelector(**values)
        except ValueError as exc:
            raise ReplayRequestError("invalid_trend_selector") from exc
        return _artifact(request, catalog.trend(_date(session), role=selector.role,
                         weighting=selector.weighting, metric=selector.metric))
    @app.api_route("/api/v1/replay/sessions/{session}/session-surface", methods=["GET", "HEAD"])
    def session_surface(session: str, request: Request) -> Response:
        values = _query(request, required={"at", "role", "weighting", "bucket_minutes", "price_step"})
        try:
            selector = SessionSurfaceSelector(role=values["role"], weighting=values["weighting"],
                bucket_minutes=int(values["bucket_minutes"]), price_step=float(values["price_step"]))
        except ValueError as exc:
            raise ReplayRequestError("invalid_session_surface_selector") from exc
        payload = catalog.session_surface(_date(session), at=_at(values["at"]), role=selector.role,
            weighting=selector.weighting, bucket_minutes=selector.bucket_minutes, price_step=selector.price_step)
        return _artifact(request, payload)

    @app.api_route("/api/v1/replay/sessions/{session}/frame", methods=["GET", "HEAD"])
    def frame(session: str, request: Request) -> Response:
        values = _query(request, required={"at"})
        return _artifact(request, catalog.frame(_date(session), _at(values["at"])))

    @app.api_route("/api/v1/replay/frames/{replay_id}", methods=["GET", "HEAD"])
    def frame_id(replay_id: str, request: Request) -> Response:
        requested = _replay_id(replay_id)
        return _artifact(request, catalog.frame(requested.astimezone(ET).date(), requested))

    return app


def create_default_app() -> FastAPI:
    settings = StorageSettings.from_env()
    catalog = ReplayCatalog(data_root=settings.data_root, storage_settings=settings, frame_minutes=int(
        os.getenv("SPX_SURFACE_REPLAY_FRAME_MINUTES", DEFAULT_FRAME_MINUTES)))
    return create_app(catalog)


def _query(request: Request, *, required: set[str] | None = None,
           allowed: set[str] | None = None) -> dict[str, str]:
    required, allowed = required or set(), allowed or required or set()
    pairs = list(request.query_params.multi_items())
    values = {key: value for key, value in pairs}
    if len(pairs) > 8 or len(values) != len(pairs) or set(values) - allowed or not required <= set(values):
        raise ReplayRequestError("invalid_query")
    return values


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ReplayRequestError("invalid_session_date") from exc
    if parsed.isoformat() != value:
        raise ReplayRequestError("invalid_session_date")
    return parsed


def _at(value: str) -> datetime:
    value = value.strip()
    if not value or len(value) > 64:
        raise ReplayRequestError("invalid_replay_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplayRequestError("invalid_replay_at") from exc
    if parsed.tzinfo is None or parsed.microsecond:
        raise ReplayRequestError("replay_at_requires_timezone" if parsed.tzinfo is None else "replay_at_subsecond_not_supported")
    return as_utc(parsed)


def _replay_id(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ReplayRequestError("invalid_replay_id") from exc
    if parsed.strftime("%Y-%m-%dT%H%M%SZ") != value:
        raise ReplayRequestError("invalid_replay_id")
    return parsed


def _integer(value: str, code: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ReplayRequestError(code) from exc


def _artifact(request: Request, payload: Mapping[str, object]) -> Response:
    etag = f'"{payload["artifact_sha256"]}"'
    headers = {"Cache-Control": "private, no-cache", "ETag": etag}
    candidates = request.headers.get("if-none-match", "").split(",")
    if any(value.strip() == "*" or hmac.compare_digest(value.strip().removeprefix("W/"), etag)
           for value in candidates if value.strip()):
        return Response(status_code=304, headers=headers)
    return JSONResponse(dict(payload), headers=headers)
