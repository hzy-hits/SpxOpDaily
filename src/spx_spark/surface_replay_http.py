"""HTTP transport and process entrypoint for the SPXW replay catalog."""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import re
import signal
import socket
import stat
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn, UnixStreamServer
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import ET
from spx_spark.marketdata import as_utc
from spx_spark.surface_dashboard_replay import (
    DEFAULT_LOOKBACK_SECONDS,
    MAX_LOOKBACK_SECONDS,
    ReplaySourceError,
)


LOGGER = logging.getLogger(__name__)

SERVICE_SCHEMA_VERSION = 1
DEFAULT_FRAME_MINUTES = 5
DEFAULT_BIND_HOST = "127.0.0.1"
DEFAULT_BIND_PORT = 8186
MAX_REQUEST_TARGET_BYTES = 2048

_SESSION_DATE_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")
_REPLAY_ID_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{6}Z\Z")
_ENTITY_TAG_RE = re.compile(r'(?:W/)?"[!#-~]+"')


class ReplayRequestError(ValueError):
    """A public replay request is malformed or outside the catalog."""

    def __init__(
        self,
        code: str,
        *,
        status: HTTPStatus = HTTPStatus.BAD_REQUEST,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


class ReplayCacheError(RuntimeError):
    """A materialized replay failed its immutable artifact checks."""


class ReplayBusyError(RuntimeError):
    """The one allowed replay materializer is already occupied."""


@dataclass(frozen=True, slots=True)
class APIResponse:
    status: HTTPStatus
    payload: dict[str, object]
    headers: tuple[tuple[str, str], ...] = ()


class ReplayCatalogProtocol(Protocol):
    data_root: Path
    frame_minutes: int

    def sessions_payload(self) -> dict[str, object]: ...

    def timeline_payload(self, session_date: date) -> dict[str, object]: ...

    def frame(
        self,
        session_date: date,
        requested: datetime,
    ) -> dict[str, object]: ...


def _parse_session_date(value: str) -> date:
    if not _SESSION_DATE_RE.fullmatch(value):
        raise ReplayRequestError("invalid_session_date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ReplayRequestError("invalid_session_date") from exc
    if parsed.isoformat() != value:
        raise ReplayRequestError("invalid_session_date")
    return parsed


def _parse_at(value: str) -> datetime:
    raw = value.strip()
    if not raw or len(raw) > 64:
        raise ReplayRequestError("invalid_replay_at")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReplayRequestError("invalid_replay_at") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayRequestError("replay_at_requires_timezone")
    requested = as_utc(parsed)
    if requested.microsecond:
        raise ReplayRequestError("replay_at_subsecond_not_supported")
    return requested


def _parse_replay_id(value: str) -> datetime:
    if not _REPLAY_ID_RE.fullmatch(value):
        raise ReplayRequestError("invalid_replay_id")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ReplayRequestError("invalid_replay_id") from exc


def _json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _if_none_match_matches(values: tuple[str, ...], current_etag: str) -> bool:
    """Use weak comparison for GET/HEAD If-None-Match entity tags."""

    if not values or sum(len(value) for value in values) > 8192:
        return False
    normalized_current = current_etag.removeprefix("W/")
    for value in values:
        for candidate in value.split(","):
            candidate = candidate.strip()
            if candidate == "*":
                return True
            if not _ENTITY_TAG_RE.fullmatch(candidate):
                continue
            if hmac.compare_digest(
                candidate.removeprefix("W/"),
                normalized_current,
            ):
                return True
    return False


class ReplayAPI:
    """Small transport adapter kept separate from catalog logic for testing."""

    def __init__(self, catalog: ReplayCatalogProtocol) -> None:
        self.catalog = catalog

    @staticmethod
    def _query(target: str) -> tuple[str, dict[str, list[str]]]:
        if (
            not target.startswith("/")
            or len(target.encode("utf-8")) > MAX_REQUEST_TARGET_BYTES
        ):
            raise ReplayRequestError("invalid_request_target")
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            raise ReplayRequestError("invalid_request_target")
        try:
            query = parse_qs(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=4,
            )
        except ValueError as exc:
            raise ReplayRequestError("invalid_query") from exc
        return parsed.path.rstrip("/") or "/", query

    @staticmethod
    def _single_query(
        query: dict[str, list[str]],
        *,
        required: frozenset[str] = frozenset(),
        allowed: frozenset[str] = frozenset(),
    ) -> dict[str, str]:
        if set(query) - allowed or not required.issubset(query):
            raise ReplayRequestError("invalid_query")
        if any(len(values) != 1 for values in query.values()):
            raise ReplayRequestError("invalid_query")
        return {key: values[0] for key, values in query.items()}

    def dispatch(self, method: str, target: str) -> APIResponse:
        if method not in {"GET", "HEAD"}:
            raise ReplayRequestError(
                "method_not_allowed",
                status=HTTPStatus.METHOD_NOT_ALLOWED,
            )
        path, query = self._query(target)
        if path == "/healthz":
            self._single_query(query)
            available = self.catalog.data_root.is_dir()
            return APIResponse(
                HTTPStatus.OK if available else HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "status": "ok" if available else "unavailable",
                    "service": "spxw-surface-replay",
                    "schema_version": SERVICE_SCHEMA_VERSION,
                },
                (("Cache-Control", "no-store"),),
            )
        if path == "/api/v1/replay/sessions":
            self._single_query(query)
            return APIResponse(
                HTTPStatus.OK,
                self.catalog.sessions_payload(),
                (("Cache-Control", "private, max-age=30"),),
            )
        timeline_match = re.fullmatch(
            r"/api/v1/replay/sessions/(\d{4}-\d{2}-\d{2})/timeline",
            path,
        )
        if timeline_match:
            values = self._single_query(
                query,
                allowed=frozenset({"step_minutes"}),
            )
            if "step_minutes" in values:
                try:
                    requested_step = int(values["step_minutes"])
                except ValueError as exc:
                    raise ReplayRequestError("invalid_step_minutes") from exc
                if requested_step != self.catalog.frame_minutes:
                    raise ReplayRequestError("unsupported_step_minutes")
            session_date = _parse_session_date(timeline_match.group(1))
            return APIResponse(
                HTTPStatus.OK,
                self.catalog.timeline_payload(session_date),
                (("Cache-Control", "private, max-age=30"),),
            )
        frame_query_match = re.fullmatch(
            r"/api/v1/replay/sessions/(\d{4}-\d{2}-\d{2})/frame",
            path,
        )
        if frame_query_match:
            values = self._single_query(
                query,
                required=frozenset({"at"}),
                allowed=frozenset({"at"}),
            )
            session_date = _parse_session_date(frame_query_match.group(1))
            requested = _parse_at(values["at"])
            return self._frame_response(session_date, requested)
        frame_match = re.fullmatch(
            r"/api/v1/replay/frames/(\d{4}-\d{2}-\d{2}T\d{6}Z)",
            path,
        )
        if frame_match:
            self._single_query(query)
            requested = _parse_replay_id(frame_match.group(1))
            return self._frame_response(requested.astimezone(ET).date(), requested)
        raise ReplayRequestError("route_not_found", status=HTTPStatus.NOT_FOUND)

    def _frame_response(
        self,
        session_date: date,
        requested: datetime,
    ) -> APIResponse:
        payload = self.catalog.frame(session_date, requested)
        artifact_hash = str(payload["artifact_sha256"])
        return APIResponse(
            HTTPStatus.OK,
            payload,
            (
                ("Cache-Control", "private, no-cache"),
                ("ETag", f'"{artifact_hash}"'),
            ),
        )


class ReplayHTTPServer(ThreadingHTTPServer):
    # Frame generation and source hashing must finish before process shutdown;
    # detached request threads could otherwise leave an unpublished cache write.
    daemon_threads = False
    allow_reuse_address = True
    request_queue_size = 32

    def __init__(self, server_address: tuple[str, int], api: ReplayAPI) -> None:
        self.api = api
        super().__init__(server_address, ReplayRequestHandler)


def _remove_stale_unix_socket(path: Path) -> None:
    """Remove a refused Unix socket, never an active socket or another file type."""

    try:
        existing = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(existing.st_mode):
        raise OSError(f"refusing to replace non-socket path: {path}")
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(0.25)
    try:
        probe.connect(str(path))
    except (ConnectionRefusedError, FileNotFoundError):
        pass
    except OSError as exc:
        raise OSError(f"cannot prove Unix socket is stale: {path}") from exc
    else:
        raise OSError(f"Unix socket is already accepting connections: {path}")
    finally:
        probe.close()
    # Check the path again after probing so a replacement cannot be unlinked.
    current = path.lstat()
    if (
        not stat.S_ISSOCK(current.st_mode)
        or current.st_dev != existing.st_dev
        or current.st_ino != existing.st_ino
    ):
        raise OSError(f"Unix socket changed while checking staleness: {path}")
    path.unlink()


class ReplayUnixHTTPServer(ThreadingMixIn, UnixStreamServer):
    daemon_threads = False
    allow_reuse_address = True
    request_queue_size = 32

    def __init__(self, socket_path: Path, api: ReplayAPI, *, mode: int = 0o660) -> None:
        self.api = api
        self.socket_path = socket_path.expanduser().resolve()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        _remove_stale_unix_socket(self.socket_path)
        super().__init__(str(self.socket_path), ReplayRequestHandler)
        try:
            self.socket_path.chmod(mode)
            bound = self.socket_path.lstat()
            self._bound_identity = (bound.st_dev, bound.st_ino)
        except Exception:
            super().server_close()
            self.socket_path.unlink(missing_ok=True)
            raise

    def server_close(self) -> None:
        super().server_close()
        try:
            current = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if (
            stat.S_ISSOCK(current.st_mode)
            and (current.st_dev, current.st_ino) == self._bound_identity
        ):
            self.socket_path.unlink()


class ReplayRequestHandler(BaseHTTPRequestHandler):
    server_version = "spx-replay"
    sys_version = ""

    @property
    def replay_server(self) -> ReplayHTTPServer | ReplayUnixHTTPServer:
        return self.server  # type: ignore[return-value]

    def _handle(self, *, include_body: bool) -> None:
        try:
            response = self.replay_server.api.dispatch(self.command, self.path)
        except ReplayRequestError as exc:
            response = APIResponse(exc.status, {"error": exc.code})
        except ReplaySourceError as exc:
            LOGGER.warning("known-clock replay frame rejected: %s", exc)
            response = APIResponse(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                {"error": "replay_frame_source_rejected"},
            )
        except ReplayCacheError as exc:
            LOGGER.error("replay cache integrity failure: %s", exc)
            response = APIResponse(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "replay_cache_integrity_failure"},
            )
        except ReplayBusyError:
            response = APIResponse(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "replay_generation_busy"},
                (("Retry-After", "2"),),
            )
        except Exception:
            LOGGER.exception("unexpected replay service failure")
            response = APIResponse(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_error"},
            )
        response_headers = dict(response.headers)
        request_etags = tuple(self.headers.get_all("If-None-Match", failobj=[]))
        not_modified = (
            response.status == HTTPStatus.OK
            and "ETag" in response_headers
            and _if_none_match_matches(request_etags, response_headers["ETag"])
        )
        if not_modified:
            response = APIResponse(
                HTTPStatus.NOT_MODIFIED,
                {},
                response.headers,
            )
            body = b""
        else:
            body = _json_bytes(response.payload)
        self.send_response(response.status.value)
        if response.status != HTTPStatus.NOT_MODIFIED:
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'none'")
        for key, value in response.headers:
            self.send_header(key, value)
        if response.status == HTTPStatus.METHOD_NOT_ALLOWED:
            self.send_header("Allow", "GET, HEAD")
        self.end_headers()
        if include_body and response.status != HTTPStatus.NOT_MODIFIED:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._handle(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(include_body=False)

    def do_POST(self) -> None:  # noqa: N802
        self._handle(include_body=True)

    def do_PUT(self) -> None:  # noqa: N802
        self._handle(include_body=True)

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle(include_body=True)

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("replay_http " + format, *args)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve recorded-clock-bounded SPXW surface session replay."
    )
    parser.add_argument("--bind-host", default=DEFAULT_BIND_HOST)
    parser.add_argument("--bind-port", type=int, default=DEFAULT_BIND_PORT)
    parser.add_argument(
        "--unix-socket",
        type=Path,
        help="serve on this Unix socket instead of TCP (takes precedence over bind host/port)",
    )
    parser.add_argument(
        "--unix-socket-mode",
        choices=("0600", "0660"),
        default="0660",
    )
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--frame-minutes", type=int, default=DEFAULT_FRAME_MINUTES)
    parser.add_argument("--lookback-seconds", type=float, default=DEFAULT_LOOKBACK_SECONDS)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    if not 1 <= args.bind_port <= 65535:
        parser.error("--bind-port must be within [1, 65535]")
    if not 1 <= args.frame_minutes <= 60:
        parser.error("--frame-minutes must be within [1, 60]")
    if not 0 < args.lookback_seconds <= MAX_LOOKBACK_SECONDS:
        parser.error(
            f"--lookback-seconds must be within (0, {MAX_LOOKBACK_SECONDS:g}]"
        )
    return args


def run(argv: list[str] | None = None) -> int:
    from spx_spark.surface_replay_service import ReplayCatalog

    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = StorageSettings.from_env()
    data_root = args.data_root or Path(settings.data_root)
    catalog = ReplayCatalog(
        data_root=data_root,
        storage_settings=settings,
        frame_minutes=args.frame_minutes,
        lookback_seconds=args.lookback_seconds,
    )
    api = ReplayAPI(catalog)
    if args.unix_socket is not None:
        server: ReplayHTTPServer | ReplayUnixHTTPServer = ReplayUnixHTTPServer(
            args.unix_socket,
            api,
            mode=int(args.unix_socket_mode, 8),
        )
        LOGGER.info(
            "serving replay API on Unix socket %s with %dm frames",
            args.unix_socket,
            args.frame_minutes,
        )
    else:
        server = ReplayHTTPServer((args.bind_host, args.bind_port), api)
        LOGGER.info(
            "serving replay API on %s:%d with %dm frames",
            args.bind_host,
            args.bind_port,
            args.frame_minutes,
        )

    def request_shutdown(_signum: int, _frame: object) -> None:
        # BaseServer.shutdown must run outside the serve_forever thread.
        threading.Thread(target=server.shutdown, daemon=True).start()

    previous_sigterm = signal.signal(signal.SIGTERM, request_shutdown)
    previous_sigint = signal.signal(signal.SIGINT, request_shutdown)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)
        server.server_close()
    return 0


def main() -> None:
    raise SystemExit(run())
