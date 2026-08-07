"""Typed environment overlay for unified notification delivery."""

from __future__ import annotations

import json
import os
from typing import Mapping

from spx_spark.app_settings import get_settings
from spx_spark.settings.loader import settings_value


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if not raw:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean-like value, got {raw!r}")


def _int(name: str, default: int) -> int:
    return int(_env(name) or default)


def _float(name: str, default: float) -> float:
    return float(_env(name) or default)


def _target_map(name: str, default: object) -> tuple[tuple[str, str, str], ...]:
    raw = _env(name)
    values = json.loads(raw) if raw else default
    if not isinstance(values, (list, tuple)):
        raise ValueError(f"{name} must be a JSON list of target objects")
    targets: list[tuple[str, str, str]] = []
    seen_sinks: set[str] = set()
    seen_keys: set[str] = set()
    for item in values:
        if not isinstance(item, Mapping):
            raise ValueError(f"{name} entries must contain sink, key, and channel")
        sink = str(item.get("sink") or "").strip()
        key = str(item.get("key") or "").strip()
        channel = str(item.get("channel") or "").strip()
        expected_channel = {
            "feishu": "feishu",
            "bark": "bark",
            "bark_friend": "bark",
        }.get(sink)
        if not key or expected_channel is None or channel != expected_channel:
            raise ValueError(f"{name} contains an invalid target")
        if sink in seen_sinks:
            raise ValueError(f"{name} contains duplicate sink {sink!r}")
        if key in seen_keys:
            raise ValueError(f"{name} contains duplicate key {key!r}")
        seen_sinks.add(sink)
        seen_keys.add(key)
        targets.append((sink, key, channel))
    return tuple(targets)


def notification_delivery_settings() -> dict[str, object]:
    """Return kwargs consumed by ``NotificationSettings.from_env``."""

    operational_database = get_settings().data_root / "spx.sqlite"
    return {
        "notification_queue_enabled": _bool(
            "SPX_NOTIFICATION_QUEUE_ENABLED",
            bool(settings_value("notification.queue_enabled")),
        ),
        "notification_database_path": _env("SPX_NOTIFICATION_DATABASE_PATH")
        or str(operational_database),
        "rust_trader_notification_owner": _bool(
            "SPX_RUST_TRADER_NOTIFICATION_OWNER",
            bool(settings_value("notification.rust_trader_notification_owner")),
        ),
        "rust_operator_notification_socket_path": (
            _env("SPX_RUST_OPERATOR_NOTIFICATION_SOCKET_PATH")
            or str(settings_value("notification.rust_operator_notification_socket_path"))
        ),
        "rust_delivery_ledger_path": (
            _env("SPX_RUST_DELIVERY_LEDGER_PATH")
            or str(settings_value("notification.rust_delivery_ledger_path"))
        ),
        "rust_operator_notification_timeout_seconds": _float(
            "SPX_RUST_OPERATOR_NOTIFICATION_TIMEOUT_SECONDS",
            float(
                settings_value(
                    "notification.rust_operator_notification_timeout_seconds"
                )
            ),
        ),
        "rust_operator_notification_max_frame_bytes": _int(
            "SPX_RUST_OPERATOR_NOTIFICATION_MAX_FRAME_BYTES",
            int(settings_value("notification.rust_operator_notification_max_frame_bytes")),
        ),
        "rust_operator_notification_target_map": _target_map(
            "SPX_RUST_OPERATOR_NOTIFICATION_TARGET_MAP_JSON",
            settings_value("notification.rust_operator_notification_target_map"),
        ),
    }
