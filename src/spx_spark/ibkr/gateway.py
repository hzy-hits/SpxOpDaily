"""Small helpers for IB Gateway socket readiness."""

from __future__ import annotations

import socket


def api_port_open(host: str, port: int, *, timeout_seconds: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False
