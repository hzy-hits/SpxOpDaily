from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import NotificationSettings
from spx_spark.notifier.policy import (
    alert_key,
    is_human_visible_alert,
    is_offhours_vol_signal_alert,
    severity_value,
)


def load_sent_state(path: str) -> dict[str, float]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    sent = payload.get("sent_at_by_key") if isinstance(payload, dict) else None
    if not isinstance(sent, dict):
        return {}
    return {str(key): float(value) for key, value in sent.items() if isinstance(value, int | float)}


def save_sent_state(path: str, sent_at_by_key: dict[str, float]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    payload = {
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "sent_at_by_key": dict(sorted(sent_at_by_key.items())),
    }
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


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
        selected.append(alert)
    return selected, sent_at_by_key


def mark_alerts_sent(
    alerts: list[dict[str, object]],
    sent_at_by_key: dict[str, float],
    settings: NotificationSettings,
    *,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(tz=timezone.utc)
    now_ts = now.timestamp()
    for alert in alerts:
        sent_at_by_key[alert_key(alert)] = now_ts
    save_sent_state(settings.state_path, sent_at_by_key)
