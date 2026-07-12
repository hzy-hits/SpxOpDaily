"""IBKR stream session helpers (reconnect wait, event logging)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone


def sleep_until_reconnect(
    *,
    host: str,
    port: int,
    delay_seconds: float,
    poll_seconds: float = 5.0,
) -> None:
    from spx_spark.ibkr.stream import deps as stream_deps

    deadline = time.monotonic() + max(delay_seconds, 0.0)
    # An already-open TCP port says nothing about an application-level IBKR
    # handshake, authentication, or client-id failure.  In that case honor
    # the complete backoff.  A port that was initially down may still wake the
    # loop early when the gateway actually appears.
    port_was_open = stream_deps.api_port_open(host, port)
    if port_was_open:
        time.sleep(max(delay_seconds, 0.0))
        return
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(poll_seconds, remaining))
        if stream_deps.api_port_open(host, port):
            return


def log_event(event: dict[str, object]) -> None:
    event.setdefault("ts", datetime.now(tz=timezone.utc).isoformat())
    print(json.dumps(event, sort_keys=True), flush=True)
