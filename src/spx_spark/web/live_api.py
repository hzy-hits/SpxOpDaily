"""Live session-surface routes on the shared FastAPI application."""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from spx_spark.config import StorageSettings
from spx_spark.marketdata import as_utc
from spx_spark.surface_live_session_models import LiveSelector, LiveSessionError
from spx_spark.surface_live_session_store import LiveSessionStateStore
from spx_spark.surface_live_session_worker import LiveSessionAccumulator
from spx_spark.surface_replay_service import DEFAULT_FRAME_MINUTES, ReplayCatalog, ReplayRequestError
from spx_spark.web.replay_api import _artifact, _query, create_app as create_replay_app


def _lifespan(accumulator: LiveSessionAccumulator, poll_seconds: float):
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        stop_event = threading.Event()
        with accumulator.store.owner_lock():
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(asyncio.to_thread(
                    accumulator.run_loop, stop_event=stop_event, poll_seconds=poll_seconds))
                try:
                    yield
                finally:
                    stop_event.set()
    return lifespan


def create_app(
    catalog: ReplayCatalog, accumulator: LiveSessionAccumulator, *, poll_seconds: float = 0.25,
) -> FastAPI:
    app = create_replay_app(catalog, lifespan=_lifespan(accumulator, poll_seconds),
                            health_payload=accumulator.health_payload)

    @app.middleware("http")
    async def live_clock_header(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-SPXW-Server-Time", as_utc(accumulator.utcnow()).isoformat())
        return response

    @app.exception_handler(LiveSessionError)
    async def live_unavailable(_request: Request, exc: LiveSessionError):
        return JSONResponse({"error": str(exc)}, status_code=503,
                            headers={"Cache-Control": "no-store", "Retry-After": "1"})

    @app.api_route("/api/v1/live/session-surface", methods=["GET", "HEAD"])
    def session_surface(request: Request) -> Response:
        values = _query(request, required={"role", "weighting", "bucket_minutes", "price_step"})
        try:
            selector = LiveSelector(role=values["role"], weighting=values["weighting"],
                bucket_minutes=int(values["bucket_minutes"]), price_step=float(values["price_step"]))
        except (TypeError, ValueError) as exc:
            raise ReplayRequestError("invalid_live_selector") from exc
        payload = accumulator.session_surface(selector, now=as_utc(accumulator.utcnow()))
        return _artifact(request, payload, cache_control="private, no-store",
            extra_headers={"X-SPXW-Server-Time": str(payload["server_time"])})

    return app


def create_default_app() -> FastAPI:
    settings = StorageSettings.from_env()
    publish = Path(settings.data_root) / "published" / "spxw-surface"
    snapshot = Path(os.getenv("SPX_LIVE_SESSION_INPUT_PATH", str(publish / "snapshot.json")))
    state = Path(os.getenv("SPX_LIVE_SESSION_STATE_ROOT", str(publish / "live/policy=live-v2/bucket=1m")))
    catalog = ReplayCatalog(data_root=settings.data_root, storage_settings=settings,
        frame_minutes=int(os.getenv("SPX_SURFACE_REPLAY_FRAME_MINUTES", DEFAULT_FRAME_MINUTES)))
    accumulator = LiveSessionAccumulator(snapshot_path=snapshot,
        state_store=LiveSessionStateStore(state))
    return create_app(catalog, accumulator,
        poll_seconds=float(os.getenv("SPX_LIVE_SESSION_POLL_SECONDS", "0.25")))
