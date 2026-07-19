"""Unix-socket HTTP service for the live SPXW Session Canvas."""

from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import stat
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import ThreadingMixIn, UnixStreamServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from spx_spark.config import StorageSettings
from spx_spark.surface_live_session_models import LiveSelector, LiveSessionError
from spx_spark.surface_live_session_store import LiveSessionStateStore
from spx_spark.surface_live_session_worker import (
    DEFAULT_POLL_SECONDS,
    LiveSessionAccumulator,
)
from spx_spark.surface_replay_http import (
    _if_none_match_matches,
    _remove_stale_unix_socket,
)


LOGGER = logging.getLogger(__name__)
MAX_REQUEST_TARGET_BYTES = 2_048


class LiveRequestError(ValueError):
    def __init__(self, code: str, *, status: HTTPStatus = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True, slots=True)
class LiveResponse:
    status: HTTPStatus
    payload: dict[str, object]
    headers: tuple[tuple[str, str], ...] = ()


class LiveAPI:
    def __init__(
        self,
        accumulator: LiveSessionAccumulator,
        *,
        utcnow=lambda: datetime.now(tz=timezone.utc),
    ) -> None:
        self.accumulator = accumulator
        self.utcnow = utcnow

    @staticmethod
    def _target(target: str) -> tuple[str, dict[str, list[str]]]:
        if not target.startswith("/") or len(target.encode("utf-8")) > MAX_REQUEST_TARGET_BYTES:
            raise LiveRequestError("invalid_request_target")
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            raise LiveRequestError("invalid_request_target")
        try:
            query = parse_qs(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=4,
            )
        except ValueError as exc:
            raise LiveRequestError("invalid_query") from exc
        return parsed.path.rstrip("/") or "/", query

    @staticmethod
    def _single(
        query: dict[str, list[str]],
        *,
        required: frozenset[str] = frozenset(),
    ) -> dict[str, str]:
        if set(query) != set(required) or any(len(values) != 1 for values in query.values()):
            raise LiveRequestError("invalid_query")
        return {key: values[0] for key, values in query.items()}

    def dispatch(self, method: str, target: str) -> LiveResponse:
        if method not in {"GET", "HEAD"}:
            raise LiveRequestError(
                "method_not_allowed",
                status=HTTPStatus.METHOD_NOT_ALLOWED,
            )
        path, query = self._target(target)
        server_time = self.utcnow().astimezone(timezone.utc)
        time_header = (("X-SPXW-Server-Time", server_time.isoformat()),)
        if path == "/healthz":
            self._single(query)
            return LiveResponse(
                HTTPStatus.OK,
                self.accumulator.health_payload(),
                (("Cache-Control", "no-store"), *time_header),
            )
        if path == "/api/v1/live/session-surface":
            values = self._single(
                query,
                required=frozenset(
                    {"role", "weighting", "bucket_minutes", "price_step"}
                ),
            )
            try:
                selector = LiveSelector(
                    role=values["role"],
                    weighting=values["weighting"],
                    bucket_minutes=int(values["bucket_minutes"]),
                    price_step=float(values["price_step"]),
                )
            except (TypeError, ValueError) as exc:
                raise LiveRequestError("invalid_live_selector") from exc
            payload = self.accumulator.session_surface(selector, now=server_time)
            # The accumulator uses this same cutoff for created_at/server_time;
            # keep the precision-bearing response header exactly aligned.
            response_time = str(payload["server_time"])
            return LiveResponse(
                HTTPStatus.OK,
                payload,
                (
                    ("Cache-Control", "private, no-store"),
                    ("ETag", f'"{payload["artifact_sha256"]}"'),
                    ("X-SPXW-Server-Time", response_time),
                ),
            )
        raise LiveRequestError("route_not_found", status=HTTPStatus.NOT_FOUND)


class LiveUnixHTTPServer(ThreadingMixIn, UnixStreamServer):
    daemon_threads = False
    allow_reuse_address = True
    request_queue_size = 16

    def __init__(self, socket_path: Path, api: LiveAPI, *, mode: int = 0o660) -> None:
        self.api = api
        self.socket_path = socket_path.expanduser().resolve()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _remove_stale_unix_socket(self.socket_path)
        super().__init__(str(self.socket_path), LiveRequestHandler)
        try:
            self.socket_path.chmod(mode)
            bound = self.socket_path.lstat()
            if not stat.S_ISSOCK(bound.st_mode):
                raise OSError("live API bind path is not a Unix socket")
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


class LiveRequestHandler(BaseHTTPRequestHandler):
    server_version = "spx-live-surface"
    sys_version = ""

    @property
    def live_server(self) -> LiveUnixHTTPServer:
        return self.server  # type: ignore[return-value]

    def _handle(self, *, include_body: bool) -> None:
        try:
            response = self.live_server.api.dispatch(self.command, self.path)
        except LiveRequestError as exc:
            now = datetime.now(tz=timezone.utc).isoformat()
            response = LiveResponse(
                exc.status,
                {"error": exc.code},
                (("Cache-Control", "no-store"), ("X-SPXW-Server-Time", now)),
            )
        except LiveSessionError as exc:
            LOGGER.info("live session unavailable: %s", exc)
            now = datetime.now(tz=timezone.utc).isoformat()
            response = LiveResponse(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": str(exc)},
                (
                    ("Cache-Control", "no-store"),
                    ("Retry-After", "1"),
                    ("X-SPXW-Server-Time", now),
                ),
            )
        except Exception:
            LOGGER.exception("unexpected live surface failure")
            now = datetime.now(tz=timezone.utc).isoformat()
            response = LiveResponse(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_error"},
                (("Cache-Control", "no-store"), ("X-SPXW-Server-Time", now)),
            )
        headers = dict(response.headers)
        etags = tuple(self.headers.get_all("If-None-Match", failobj=[]))
        not_modified = (
            response.status == HTTPStatus.OK
            and "ETag" in headers
            and _if_none_match_matches(etags, headers["ETag"])
        )
        if not_modified:
            response = LiveResponse(HTTPStatus.NOT_MODIFIED, {}, response.headers)
            body = b""
        else:
            body = json.dumps(
                response.payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
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

    do_PUT = do_POST
    do_DELETE = do_POST

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("live_surface_http " + format, *args)


def _default_paths(settings: StorageSettings) -> tuple[Path, Path, Path]:
    publish = Path(settings.data_root).expanduser() / "published" / "spxw-surface"
    return (
        publish / "snapshot.json",
        publish / "live",
        publish / "runtime" / "live" / "live-api.sock",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the persistent live SPXW Session Canvas.")
    parser.add_argument("--input-path", "--snapshot-path", dest="input_path", type=Path)
    parser.add_argument("--state-root", "--state-dir", dest="state_root", type=Path)
    parser.add_argument("--unix-socket", type=Path)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--bucket-minutes", type=int, default=5)
    parser.add_argument("--price-step", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive and finite")
    if args.bucket_minutes != 5:
        parser.error("--bucket-minutes only supports 5")
    if not math.isclose(args.price_step, 5.0, rel_tol=0.0, abs_tol=1e-12):
        parser.error("--price-step only supports 5")
    return args


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = StorageSettings.from_env()
    default_input, default_state, default_socket = _default_paths(settings)
    input_path = args.input_path or default_input
    state_root = args.state_root or default_state
    socket_path = args.unix_socket or default_socket
    store = LiveSessionStateStore(state_root)
    stop_event = threading.Event()
    with store.owner_lock():
        accumulator = LiveSessionAccumulator(
            snapshot_path=input_path,
            state_store=store,
        )
        api = LiveAPI(accumulator)
        server = LiveUnixHTTPServer(socket_path, api, mode=0o660)
        fatal: list[BaseException] = []

        def worker_main() -> None:
            try:
                accumulator.run_loop(
                    stop_event=stop_event,
                    poll_seconds=args.poll_seconds,
                )
            except BaseException as exc:  # propagate worker death to process owner
                fatal.append(exc)
                stop_event.set()
                threading.Thread(target=server.shutdown, daemon=True).start()

        worker = threading.Thread(
            target=worker_main,
            name="spxw-live-session-accumulator",
            daemon=False,
        )
        worker.start()

        def request_shutdown(_signum: int, _frame: object) -> None:
            stop_event.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

        previous_sigterm = signal.signal(signal.SIGTERM, request_shutdown)
        previous_sigint = signal.signal(signal.SIGINT, request_shutdown)
        try:
            server.serve_forever(poll_interval=0.25)
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm)
            signal.signal(signal.SIGINT, previous_sigint)
            stop_event.set()
            server.server_close()
            worker.join(timeout=max(args.poll_seconds * 2, 2.0))
        if worker.is_alive():
            raise RuntimeError("live accumulator worker did not stop")
        if fatal:
            raise RuntimeError("live accumulator worker failed") from fatal[0]
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
