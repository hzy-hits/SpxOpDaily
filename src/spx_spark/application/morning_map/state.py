"""Morning-map send-window gating and idempotent sent markers."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from spx_spark.application.morning_map.constants import ET_WINDOW_END, ET_WINDOW_START
from spx_spark.config import NY_TZ, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR

def default_state_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_MORNING_MAP_STATE_PATH") or str(
        Path(settings.data_root) / "latest" / "morning_map_state.json"
    )


def within_send_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(NY_TZ)
    if not DEFAULT_MARKET_CALENDAR.is_trading_day(local.date()):
        return False
    current = local.time()
    return ET_WINDOW_START <= current < ET_WINDOW_END


def already_sent(state_path: str, trading_date: str) -> bool:
    path = Path(state_path)
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    return payload.get("last_sent_date") == trading_date


def mark_sent(state_path: str, trading_date: str) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"last_sent_date": trading_date}, ensure_ascii=False),
        encoding="utf-8",
    )
