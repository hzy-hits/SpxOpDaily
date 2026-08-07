"""FastAPI transport for Schwab OAuth callback and local market-data gateway."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from spx_spark.schwab.gateway import (
    SchwabGatewayRequestError,
    SchwabGatewayUnavailable,
    SchwabSessionManager,
)
from spx_spark.schwab.oauth_service import (
    OAuthCallbackError,
    OAuthCoordinator,
    is_loopback_host,
)


CALLBACK_HEADERS = {
    "Cache-Control": "no-store", "Pragma": "no-cache", "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
}
GATEWAY_HEADERS = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}


def _event(event: str, *, ok: bool, **facts: object) -> None:
    print(json.dumps({"event": event, "ok": ok, **facts}, sort_keys=True),
          file=sys.stdout if ok else sys.stderr, flush=True)


def _html(status: int, title: str, message: str) -> HTMLResponse:
    body = ("<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{title}</title></head><body><h1>{title}</h1><p>{message}</p></body></html>")
    return HTMLResponse(body, status_code=status)


def create_app(
    coordinator: OAuthCoordinator, *, gateway: bool = False,
    stream_health: Callable[[], dict[str, Any]] | None = None,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    headers = GATEWAY_HEADERS if gateway else CALLBACK_HEADERS

    @app.middleware("http")
    async def protect_and_redact(request: Request, call_next):
        target = request.scope.get("raw_path", b"") + b"?" + request.scope.get("query_string", b"")
        if gateway and len(target) > 65_536:
            response: Response = JSONResponse({"ok": False, "error": "request_target_too_large"}, status_code=414)
        elif gateway and not is_loopback_host(request.url.hostname or ""):
            response = JSONResponse({"ok": False, "error": "loopback_host_required"}, status_code=403)
        else:
            response = await call_next(request)
        response.headers.update(headers)
        return response

    @app.exception_handler(StarletteHTTPException)
    async def protocol_error(_request: Request, exc: StarletteHTTPException):
        code = "not_found" if exc.status_code == 404 else "method_not_allowed"
        return JSONResponse({"ok": False, "error": code}, status_code=exc.status_code)

    @app.exception_handler(Exception)
    async def internal_error(_request: Request, exc: Exception):
        _event("schwab_http_handler_error", ok=False, error_type=type(exc).__name__)
        return JSONResponse({"ok": False, "error": "internal_error"}, status_code=500)

    if gateway:
        _add_gateway_routes(app, coordinator.manager, stream_health)
    else:
        _add_callback_routes(app, coordinator)
    return app


def _add_callback_routes(app: FastAPI, coordinator: OAuthCoordinator) -> None:
    callback_path = urlsplit(coordinator.settings.callback_url).path or "/"

    @app.get("/healthz")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get(callback_path)
    def callback(request: Request) -> Response:
        query = request.scope.get("query_string", b"").decode("ascii")
        try:
            coordinator.complete(query)
        except OAuthCallbackError as exc:
            _event("schwab_oauth_callback", ok=False, status=exc.status)
            return _html(exc.status, "Schwab authorization failed", str(exc))
        _event("schwab_oauth_callback", ok=True, status=200)
        return _html(200, "Schwab authorization complete",
                     "The token was stored on Oracle. You may close this tab.")


def _add_gateway_routes(
    app: FastAPI, manager: SchwabSessionManager,
    stream_health: Callable[[], dict[str, Any]] | None,
) -> None:
    @app.get("/livez")
    def live() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/healthz")
    def health() -> JSONResponse:
        state = manager.health()
        payload = state.to_dict()
        if stream_health is not None:
            payload["stream"] = stream_health()
        return JSONResponse(payload, status_code=200 if state.ready else 503)

    @app.get("/marketdata/v1/quotes")
    @app.get("/marketdata/v1/chains")
    def market_data(request: Request) -> Response:
        return _market_data(manager, request)


def _market_data(manager: SchwabSessionManager, request: Request) -> Response:
    try:
        params = parse_qsl(request.scope.get("query_string", b"").decode("ascii"),
                           keep_blank_values=True, max_num_fields=100)
        result = manager.request(request.url.path, params)
    except SchwabGatewayUnavailable:
        return JSONResponse({"ok": False, "error": "schwab_auth_not_ready"}, status_code=503)
    except (SchwabGatewayRequestError, ValueError) as exc:
        _event("schwab_gateway_request", ok=False, error_type=type(exc).__name__)
        return JSONResponse({"ok": False, "error": "schwab_request_failed"}, status_code=502)
    except Exception as exc:  # noqa: BLE001 - provider details must not escape
        _event("schwab_gateway_request", ok=False, error_type=type(exc).__name__)
        return JSONResponse({"ok": False, "error": "schwab_request_failed"}, status_code=502)
    if result.status == 401:
        _event("schwab_reauthorization_required", ok=False, status=401)
    return Response(result.body, status_code=result.status,
                    headers={"Content-Type": result.content_type})
