from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import NotificationSettings
from spx_spark.notifier.policy import (
    alert_key,
    is_human_visible_alert,
    is_offhours_vol_signal_alert,
    severity_value,
)
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


# Magnitude-bucketed kinds: their dedup_group encodes direction + bucket
# ("up:3"), so a slowly drifting value opens a fresh cooldown slot on every
# bucket step. The kind-level rate limit caps them to one push per window per
# kind+instrument; a >= 2 bucket jump, a direction flip, or critical severity
# breaks through.
BUCKET_RATE_LIMITED_KINDS = frozenset(
    {
        "put_skew_steepening_5m",
        "atm_iv_jump_5m",
        "iv_surface_shift_5m",
        "iv_surface_shift_1h",
        "atm_iv_change_1h",
        "price_move_from_close",
        "spxw_position_book_pnl",
    }
)

BUCKET_JUMP_OVERRIDE_STEPS = 2
INTRADAY_SHOCK_CORRELATION_SECONDS = 15 * 60

_DIRECTION_BUCKET_RE = re.compile(r"^(up|down):(\d+)$")


def signed_bucket(alert: dict[str, object]) -> float | None:
    """Signed bucket from a 'up:N'/'down:N' dedup group; None for other forms."""
    match = _DIRECTION_BUCKET_RE.match(str(alert.get("dedup_group") or ""))
    if match is None:
        return None
    value = float(match.group(2))
    return value if match.group(1) == "up" else -value


def _rate_limit_keys(alert: dict[str, object]) -> tuple[str, str]:
    base = f"{alert.get('kind')}|{alert.get('instrument_id')}"
    return f"ratelimit_at|{base}", f"ratelimit_bucket|{base}"


def kind_rate_limit_blocks(
    alert: dict[str, object],
    sent_at_by_key: dict[str, float],
    *,
    now_ts: float,
    rate_limit_seconds: float,
) -> bool:
    if rate_limit_seconds <= 0:
        return False
    if str(alert.get("kind") or "") not in BUCKET_RATE_LIMITED_KINDS:
        return False
    if severity_value(alert.get("severity")) >= severity_value("critical"):
        return False
    at_key, bucket_key = _rate_limit_keys(alert)
    last_ts = sent_at_by_key.get(at_key)
    if last_ts is None or now_ts - last_ts >= rate_limit_seconds:
        return False
    bucket = signed_bucket(alert)
    previous_bucket = sent_at_by_key.get(bucket_key)
    if bucket is not None and previous_bucket is not None:
        if (bucket > 0) != (previous_bucket > 0):
            return False
        if abs(bucket - previous_bucket) >= BUCKET_JUMP_OVERRIDE_STEPS:
            return False
    return True


def mark_rate_limit_sent(
    alert: dict[str, object],
    sent_at_by_key: dict[str, float],
    *,
    now_ts: float,
) -> None:
    if str(alert.get("kind") or "") not in BUCKET_RATE_LIMITED_KINDS:
        return
    at_key, bucket_key = _rate_limit_keys(alert)
    sent_at_by_key[at_key] = now_ts
    bucket = signed_bucket(alert)
    if bucket is not None:
        sent_at_by_key[bucket_key] = bucket


def recent_intraday_shock_blocks_price_move(
    alert: dict[str, object],
    sent_at_by_key: dict[str, float],
    *,
    now_ts: float,
) -> bool:
    """Avoid a second fixed-cycle push for a shock already sent in real time."""

    if str(alert.get("kind") or "") != "price_move_from_close":
        return False
    if str(alert.get("instrument_id") or "") not in {"index:SPX", "future:ES"}:
        return False
    dedup_group = str(alert.get("dedup_group") or "")
    direction = dedup_group.partition(":")[0]
    if direction not in {"up", "down"}:
        return False
    prefix = "intraday_price_shock|index:SPX|spx_shock:"
    direction_marker = f":{direction}:"
    return any(
        key.startswith(prefix)
        and direction_marker in key
        and key.endswith(":shock")
        # A concurrent fast-path write may be a few seconds newer than the
        # full alert payload's market timestamp.
        and -60 <= now_ts - sent_at < INTRADAY_SHOCK_CORRELATION_SECONDS
        for key, sent_at in sent_at_by_key.items()
    )


def _load_state_payload(path: str) -> dict[str, object]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_sent_state(path: str) -> dict[str, float]:
    payload = _load_state_payload(path)
    sent = payload.get("sent_at_by_key")
    if not isinstance(sent, dict):
        return {}
    return {str(key): float(value) for key, value in sent.items() if isinstance(value, int | float)}


def load_acknowledged_event_ids(path: str) -> tuple[str, ...]:
    payload = _load_state_payload(path)
    event_ids = payload.get("acknowledged_event_ids")
    if not isinstance(event_ids, list):
        return ()
    return tuple(sorted({str(event_id) for event_id in event_ids if event_id}))


def _write_sent_state_unlocked(
    path: str,
    sent_at_by_key: dict[str, float],
    acknowledged_event_ids: set[str],
) -> None:
    state_path = Path(path)
    payload = {
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "sent_at_by_key": dict(sorted(sent_at_by_key.items())),
        "acknowledged_event_ids": sorted(acknowledged_event_ids),
    }
    atomic_write_json_secure(state_path, payload)


def save_sent_state(
    path: str,
    sent_at_by_key: dict[str, float],
    *,
    acknowledged_event_ids: tuple[str, ...] = (),
) -> None:
    state_path = Path(path)
    with exclusive_state_lock(state_path):
        merged_sent_at = load_sent_state(path)
        for key, value in sent_at_by_key.items():
            merged_sent_at[key] = max(value, merged_sent_at.get(key, value))
        existing_event_ids = set(load_acknowledged_event_ids(path))
        existing_event_ids.update(acknowledged_event_ids)
        _write_sent_state_unlocked(path, merged_sent_at, existing_event_ids)


def select_alerts_for_notification(
    payload: dict[str, object],
    settings: NotificationSettings,
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, object]], dict[str, float]]:
    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        return [], load_sent_state(settings.state_path)

    now = now or datetime.now(tz=timezone.utc)
    now_ts = now.timestamp()
    min_rank = severity_value(settings.min_severity)
    sent_at_by_key = load_sent_state(settings.state_path)

    selected: list[dict[str, object]] = []
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        if not is_human_visible_alert(alert):
            continue
        # Off-hours vol repricing signals bypass the severity floor: quiet
        # windows stamp them low/medium, which used to filter them out here
        # before the direct-push path could see them.
        if severity_value(alert.get("severity")) < min_rank and not is_offhours_vol_signal_alert(
            alert, payload
        ):
            continue
        key = alert_key(alert)
        previous_ts = sent_at_by_key.get(key)
        if previous_ts is not None and now_ts - previous_ts < settings.cooldown_seconds:
            continue
        if recent_intraday_shock_blocks_price_move(
            alert,
            sent_at_by_key,
            now_ts=now_ts,
        ):
            continue
        if kind_rate_limit_blocks(
            alert,
            sent_at_by_key,
            now_ts=now_ts,
            rate_limit_seconds=settings.kind_rate_limit_seconds,
        ):
            continue
        selected.append(alert)
    return selected, sent_at_by_key


def mark_alerts_sent(
    alerts: list[dict[str, object]],
    sent_at_by_key: dict[str, float],
    settings: NotificationSettings,
    *,
    now: datetime | None = None,
    acknowledged_event_ids: tuple[str, ...] = (),
) -> None:
    now = now or datetime.now(tz=timezone.utc)
    now_ts = now.timestamp()
    state_path = Path(settings.state_path)
    with exclusive_state_lock(state_path):
        merged_sent_at = load_sent_state(settings.state_path)
        for key, value in sent_at_by_key.items():
            merged_sent_at[key] = max(value, merged_sent_at.get(key, value))
        for alert in alerts:
            merged_sent_at[alert_key(alert)] = now_ts
            mark_rate_limit_sent(alert, merged_sent_at, now_ts=now_ts)
        merged_event_ids = set(load_acknowledged_event_ids(settings.state_path))
        merged_event_ids.update(acknowledged_event_ids)
        _write_sent_state_unlocked(
            settings.state_path,
            merged_sent_at,
            merged_event_ids,
        )
