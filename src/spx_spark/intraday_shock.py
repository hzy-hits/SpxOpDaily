"""Lightweight deterministic SPX/ES shock and reversal monitor.

This path intentionally stays separate from the full alert engine.  It runs
frequently, reads only the latest live IBKR SPX/ES anchors, persists a compact
price path, and sends confirmed state transitions without giving an LLM veto
authority.  The alerts are observations, not automatic trade entries.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.alert_model import Alert
from spx_spark.alert_profile import active_window
from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings, env_float, env_int
from spx_spark.data_platform.integration import (
    IntradayResearchResult,
    persist_intraday_evaluation,
    prepare_intraday_evaluation,
    record_notification_result,
    record_outcome_rows,
)
from spx_spark.data_platform.ids import deterministic_id
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.greek_shadow import sample_zero_dte_greeks_shadow
from spx_spark.intraday_event_outcomes import (
    IntradayEventOutcomeSettings,
    IntradayEventOutcomeTracker,
    SynchronizedSPXSample,
)
from spx_spark.intraday_strategy import (
    FLIP_RECLAIM_CALL_KIND,
    STRATEGY_KINDS,
    IntradayPathSignal,
    IntradayStrategySettings,
    advance_intraday_strategy,
    mark_strategy_alert_attempts,
    structure_from_options_map,
    unavailable_structure,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import MarketDataQuality, Provider, Quote, as_utc
from spx_spark.notifier import notify_payload
from spx_spark.notifier.policy import alert_key
from spx_spark.notifier.state import load_acknowledged_event_ids
from spx_spark.options_map import build_options_map
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.runtime_config import runtime_value
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision


STATE_SCHEMA_VERSION = 1
SHOCK_KIND = "intraday_price_shock"
RECLAIM_KIND = "intraday_price_reclaim"


@dataclass(frozen=True)
class IntradayShockSettings:
    state_path: str
    one_minute_seconds: int = 60
    three_minute_seconds: int = 180
    one_minute_threshold_bps: float = 20.0
    three_minute_threshold_bps: float = 35.0
    es_confirm_ratio: float = 0.50
    max_spx_age_seconds: float = 15.0
    max_es_age_seconds: float = 10.0
    max_anchor_skew_seconds: float = 5.0
    reclaim_window_seconds: int = 300
    event_expiry_seconds: int = 600
    reclaim_fraction: float = 0.60
    es_reclaim_fraction: float = 0.40
    reclaim_hold_fraction: float = 0.55
    es_reclaim_hold_fraction: float = 0.35
    reclaim_confirm_samples: int = 2
    completion_hold_seconds: int = 60
    rearm_recovery_fraction: float = 0.40
    rearm_neutral_seconds: int = 300
    retry_seconds: int = 30

    @classmethod
    def from_env(cls) -> "IntradayShockSettings":
        data_root = (
            os.getenv("MARKET_DATA_DATA_ROOT")
            or os.getenv("MAINTENANCE_DATA_ROOT")
            or str(runtime_value("maintenance.data_root"))
        )
        return cls(
            state_path=os.getenv("ALERT_INTRADAY_SHOCK_STATE_PATH")
            or f"{data_root.rstrip('/')}/latest/intraday_shock_state.json",
            one_minute_seconds=env_int(
                "ALERT_INTRADAY_SHOCK_1M_SECONDS",
                int(runtime_value("intraday_shock.one_minute_seconds")),
            ),
            three_minute_seconds=env_int(
                "ALERT_INTRADAY_SHOCK_3M_SECONDS",
                int(runtime_value("intraday_shock.three_minute_seconds")),
            ),
            one_minute_threshold_bps=env_float(
                "ALERT_INTRADAY_SHOCK_1M_BPS",
                float(runtime_value("intraday_shock.one_minute_threshold_bps")),
            ),
            three_minute_threshold_bps=env_float(
                "ALERT_INTRADAY_SHOCK_3M_BPS",
                float(runtime_value("intraday_shock.three_minute_threshold_bps")),
            ),
            es_confirm_ratio=env_float(
                "ALERT_INTRADAY_SHOCK_ES_CONFIRM_RATIO",
                float(runtime_value("intraday_shock.es_confirm_ratio")),
            ),
            max_spx_age_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_SPX_MAX_AGE_SECONDS",
                float(runtime_value("intraday_shock.max_spx_age_seconds")),
            ),
            max_es_age_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_ES_MAX_AGE_SECONDS",
                float(runtime_value("intraday_shock.max_es_age_seconds")),
            ),
            max_anchor_skew_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_MAX_ANCHOR_SKEW_SECONDS",
                float(runtime_value("intraday_shock.max_anchor_skew_seconds")),
            ),
            reclaim_window_seconds=env_int(
                "ALERT_INTRADAY_RECLAIM_WINDOW_SECONDS",
                int(runtime_value("intraday_shock.reclaim_window_seconds")),
            ),
            event_expiry_seconds=env_int(
                "ALERT_INTRADAY_EVENT_EXPIRY_SECONDS",
                int(runtime_value("intraday_shock.event_expiry_seconds")),
            ),
            reclaim_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_FRACTION",
                float(runtime_value("intraday_shock.reclaim_fraction")),
            ),
            es_reclaim_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_ES_FRACTION",
                float(runtime_value("intraday_shock.es_reclaim_fraction")),
            ),
            reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_HOLD_FRACTION",
                float(runtime_value("intraday_shock.reclaim_hold_fraction")),
            ),
            es_reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_ES_HOLD_FRACTION",
                float(runtime_value("intraday_shock.es_reclaim_hold_fraction")),
            ),
            reclaim_confirm_samples=env_int(
                "ALERT_INTRADAY_RECLAIM_CONFIRM_SAMPLES",
                int(runtime_value("intraday_shock.reclaim_confirm_samples")),
            ),
            completion_hold_seconds=env_int(
                "ALERT_INTRADAY_COMPLETION_HOLD_SECONDS",
                int(runtime_value("intraday_shock.completion_hold_seconds")),
            ),
            rearm_recovery_fraction=env_float(
                "ALERT_INTRADAY_REARM_RECOVERY_FRACTION",
                float(runtime_value("intraday_shock.rearm_recovery_fraction")),
            ),
            rearm_neutral_seconds=env_int(
                "ALERT_INTRADAY_REARM_NEUTRAL_SECONDS",
                int(runtime_value("intraday_shock.rearm_neutral_seconds")),
            ),
            retry_seconds=env_int(
                "ALERT_INTRADAY_DELIVERY_RETRY_SECONDS",
                int(runtime_value("intraday_shock.retry_seconds")),
            ),
        )


@dataclass(frozen=True)
class PriceSample:
    at: datetime
    spx: float
    es: float
    spx_source_at: datetime | None = None
    es_source_at: datetime | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "at": as_utc(self.at).isoformat(),
            "spx": self.spx,
            "es": self.es,
            "spx_source_at": as_utc(self.spx_source_at).isoformat()
            if self.spx_source_at is not None
            else None,
            "es_source_at": as_utc(self.es_source_at).isoformat()
            if self.es_source_at is not None
            else None,
        }


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return as_utc(parsed)


def _sample_from_dict(value: object) -> PriceSample | None:
    if not isinstance(value, dict):
        return None
    at = _parse_datetime(value.get("at"))
    spx = value.get("spx")
    es = value.get("es")
    if at is None or not isinstance(spx, int | float) or not isinstance(es, int | float):
        return None
    if float(spx) <= 0 or float(es) <= 0:
        return None
    return PriceSample(
        at=at,
        spx=float(spx),
        es=float(es),
        spx_source_at=_parse_datetime(value.get("spx_source_at")),
        es_source_at=_parse_datetime(value.get("es_source_at")),
    )


def empty_monitor_state(session_date: str) -> dict[str, object]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "session_date": session_date,
        "samples": [],
        "active_event": None,
        "rearm": None,
        "last_event": None,
        "updated_at": None,
    }


def load_monitor_state(path: str, *, session_date: str) -> dict[str, object]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_monitor_state(session_date)
    if not isinstance(payload, dict) or payload.get("session_date") != session_date:
        return empty_monitor_state(session_date)
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        return empty_monitor_state(session_date)
    return payload


def _event_datetime(event: dict[str, object], field: str) -> datetime | None:
    return _parse_datetime(event.get(field))


def _bps(current: float, anchor: float) -> float:
    return (current / anchor - 1.0) * 10_000.0


def _event_id(session_date: str, direction: str, anchor_at: datetime) -> str:
    minute = as_utc(anchor_at).strftime("%H%M")
    return f"spx_shock:{session_date.replace('-', '')}:{direction}:{minute}"


def _candidate_for_horizon(
    history: list[PriceSample],
    current: PriceSample,
    *,
    horizon_seconds: int,
    threshold_bps: float,
    es_confirm_ratio: float,
) -> dict[str, object] | None:
    eligible = [
        sample
        for sample in history
        if 0 < (current.at - sample.at).total_seconds() <= horizon_seconds
    ]
    if not eligible:
        return None

    down_anchor = max(eligible, key=lambda sample: sample.spx)
    up_anchor = min(eligible, key=lambda sample: sample.spx)
    candidates: list[dict[str, object]] = []
    for direction, anchor in (("down", down_anchor), ("up", up_anchor)):
        spx_move = _bps(current.spx, anchor.spx)
        es_move = _bps(current.es, anchor.es)
        direction_ok = (
            spx_move <= -threshold_bps if direction == "down" else spx_move >= threshold_bps
        )
        es_ok = (
            es_move < 0 and abs(es_move) >= abs(spx_move) * es_confirm_ratio
            if direction == "down"
            else es_move > 0 and abs(es_move) >= abs(spx_move) * es_confirm_ratio
        )
        if direction_ok and es_ok:
            candidates.append(
                {
                    "direction": direction,
                    "anchor": anchor,
                    "spx_move_bps": spx_move,
                    "es_move_bps": es_move,
                    "threshold_bps": threshold_bps,
                    "horizon_seconds": horizon_seconds,
                    "score": abs(spx_move) / threshold_bps,
                }
            )
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: float(candidate["score"]))


def _find_shock_candidate(
    history: list[PriceSample],
    current: PriceSample,
    settings: IntradayShockSettings,
) -> dict[str, object] | None:
    candidates = [
        candidate
        for candidate in (
            _candidate_for_horizon(
                history,
                current,
                horizon_seconds=settings.one_minute_seconds,
                threshold_bps=settings.one_minute_threshold_bps,
                es_confirm_ratio=settings.es_confirm_ratio,
            ),
            _candidate_for_horizon(
                history,
                current,
                horizon_seconds=settings.three_minute_seconds,
                threshold_bps=settings.three_minute_threshold_bps,
                es_confirm_ratio=settings.es_confirm_ratio,
            ),
        )
        if candidate is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: float(candidate["score"]))


def _recovery_fraction(direction: str, current: float, anchor: float, extreme: float) -> float:
    shock = anchor - extreme if direction == "down" else extreme - anchor
    if shock <= 0:
        return 0.0
    recovered = current - extreme if direction == "down" else extreme - current
    return max(recovered / shock, 0.0)


def _pending_due(event: dict[str, object], phase: str, now: datetime, retry_seconds: int) -> bool:
    if event.get(f"{phase}_delivered") is True:
        return False
    attempted_at = _event_datetime(event, f"{phase}_last_attempt_at")
    return attempted_at is None or (now - attempted_at).total_seconds() >= retry_seconds


def _shock_alert(event: dict[str, object]) -> Alert:
    direction = str(event["direction"])
    spx_move = float(event["shock_spx_bps"])
    es_move = float(event["shock_es_bps"])
    event_id = str(event["event_id"])
    duration = float(event["shock_duration_seconds"])
    direction_label = "急跌" if direction == "down" else "急拉"
    return Alert(
        severity="high",
        kind=SHOCK_KIND,
        instrument_id="index:SPX",
        title=f"SPX/ES confirmed {direction_label} {abs(spx_move):.1f} bps",
        detail=(
            f"SPX 在 {duration:.0f} 秒内{direction_label} {abs(spx_move):.1f} bps，"
            f"ES 同向 {abs(es_move):.1f} bps；这是 0DTE 波动冲击提醒，不等于自动买 "
            f"{'put' if direction == 'down' else 'call'}，等待局部极值后的结构确认。"
        ),
        provider=Provider.IBKR.value,
        quality=MarketDataQuality.LIVE.value,
        value=spx_move,
        threshold=float(event["shock_threshold_bps"]),
        source_gate="spx_es_intraday_shock_confirmed",
        dedup_group=f"{event_id}:shock",
        event_id=event_id,
        source_at=str(event.get("extreme_at") or event.get("anchor_at") or "") or None,
    )


def _reclaim_alert(event: dict[str, object]) -> Alert:
    direction = str(event["direction"])
    spx_recovery = float(event["spx_recovery_fraction"])
    es_recovery = float(event["es_recovery_fraction"])
    event_id = str(event["event_id"])
    if direction == "down":
        title = "SPX/ES V 反确认"
        detail_direction = "急跌"
        expression = "call"
    else:
        title = "SPX/ES 倒 V 回落确认"
        detail_direction = "急拉"
        expression = "put"
    return Alert(
        severity="high",
        kind=RECLAIM_KIND,
        instrument_id="index:SPX",
        title=f"{title}，SPX 收复 {spx_recovery:.0%}",
        detail=(
            f"{detail_direction}后 SPX 连续确认收复 {spx_recovery:.0%}，ES 收复 {es_recovery:.0%}；"
            f"短时反转已成立，但这只是 {expression} 剧本升温，仍需结合 flip/zero gamma/墙位，"
            "不自动生成入场。"
        ),
        provider=Provider.IBKR.value,
        quality=MarketDataQuality.LIVE.value,
        value=spx_recovery,
        threshold=float(event["reclaim_threshold"]),
        source_gate="spx_es_intraday_reclaim_confirmed",
        dedup_group=f"{event_id}:reclaim",
        event_id=event_id,
        source_at=str(event.get("reclaim_confirmed_at") or event.get("extreme_at") or "")
        or None,
    )


def _strategy_alert(signal: IntradayPathSignal) -> Alert:
    if signal.kind == FLIP_RECLAIM_CALL_KIND:
        title = f"SPX 收复 flip {_dash_level(signal.level)}，Call 路径确认"
        detail = (
            f"急跌 V 反后，SPX/ES 两组新鲜样本守住冻结 flip {_dash_level(signal.level)}；"
            f"只把回踩不破视为 0DTE call 延续入口，失效线 {_dash_level(signal.invalidation_level)}，"
            "不追价、不自动下单。Gamma 只描述放大/钉住环境，不代表涨跌方向。"
        )
        gate = "spx_es_flip_reclaim_call_confirmed"
    else:
        title = f"SPX 突破旧 Call Wall {_dash_level(signal.level)}，延续确认"
        detail = (
            f"SPX/ES 两组新鲜样本接受在突破前冻结的 call wall {_dash_level(signal.level)} 上方；"
            f"只在回踩不破时看 0DTE call，失效线 {_dash_level(signal.invalidation_level)}，"
            "第一次刺穿不算突破，不追价、不自动下单。"
        )
        gate = "spx_es_call_wall_breakout_call_confirmed"
    return Alert(
        severity="high",
        kind=signal.kind,
        instrument_id="index:SPX",
        title=title,
        detail=detail,
        provider=Provider.IBKR.value,
        quality=MarketDataQuality.LIVE.value,
        value=signal.level,
        threshold=signal.invalidation_level,
        source_gate=gate,
        dedup_group=f"{signal.event_id}:strategy",
        event_id=signal.event_id,
        source_at=signal.confirmed_at.isoformat(),
        source_event_key=(
            deterministic_id("source_event", signal.source_event_id)
            if signal.source_event_id
            else None
        ),
    )


def _dash_level(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:.1f}"


def advance_monitor_state(
    state: dict[str, object],
    sample: PriceSample,
    settings: IntradayShockSettings,
) -> tuple[dict[str, object], list[Alert]]:
    """Advance one synchronized SPX/ES sample and return due deterministic alerts."""

    state = dict(state)
    parsed_history = [
        parsed
        for item in state.get("samples", [])
        if (parsed := _sample_from_dict(item)) is not None
    ]
    if parsed_history:
        last = parsed_history[-1]
        if (
            sample.spx_source_at is not None
            and sample.es_source_at is not None
            and last.spx_source_at == sample.spx_source_at
            and last.es_source_at == sample.es_source_at
        ):
            return state, []

    history_start_seconds = max(
        settings.three_minute_seconds,
        settings.event_expiry_seconds,
    )
    parsed_history = [
        prior
        for prior in parsed_history
        if 0 <= (sample.at - prior.at).total_seconds() <= history_start_seconds
    ]
    active = state.get("active_event")
    event = dict(active) if isinstance(active, dict) else None
    rearm_raw = state.get("rearm")
    rearm = dict(rearm_raw) if isinstance(rearm_raw, dict) else None

    if event is not None:
        delivered_at = _event_datetime(event, "reclaim_delivered_at")
        if (
            delivered_at is not None
            and (sample.at - delivered_at).total_seconds() >= settings.completion_hold_seconds
        ):
            event["status"] = "completed"
            event["completed_at"] = as_utc(sample.at).isoformat()
            state["last_event"] = event
            parsed_history = [prior for prior in parsed_history if prior.at >= delivered_at]
            event = None

    if event is not None:
        anchor_at = _event_datetime(event, "anchor_at")
        extreme_times = [
            at
            for at in (
                _event_datetime(event, "extreme_at"),
                _event_datetime(event, "extreme_es_at"),
            )
            if at is not None
        ]
        latest_extreme_at = max(extreme_times) if extreme_times else anchor_at
        expiry_at = (
            max(
                anchor_at + timedelta(seconds=settings.event_expiry_seconds),
                latest_extreme_at + timedelta(seconds=settings.reclaim_window_seconds),
            )
            if anchor_at is not None and latest_extreme_at is not None
            else None
        )
        if expiry_at is None or sample.at > expiry_at:
            event["status"] = "expired"
            event["expired_at"] = as_utc(sample.at).isoformat()
            state["last_event"] = event
            rearm = {
                "direction": event.get("direction"),
                "anchor_spx": event.get("anchor_spx"),
                "extreme_spx": event.get("extreme_spx"),
                "neutral_since": None,
            }
            event = None

    if event is None and rearm is not None:
        direction = str(rearm.get("direction"))
        anchor_spx = float(rearm.get("anchor_spx") or sample.spx)
        extreme_spx = float(rearm.get("extreme_spx") or sample.spx)
        recovery = _recovery_fraction(direction, sample.spx, anchor_spx, extreme_spx)
        neutral_since = _event_datetime(rearm, "neutral_since")
        if recovery >= settings.rearm_recovery_fraction:
            if neutral_since is None:
                rearm["neutral_since"] = as_utc(sample.at).isoformat()
            elif (sample.at - neutral_since).total_seconds() >= settings.rearm_neutral_seconds:
                parsed_history = [prior for prior in parsed_history if prior.at >= neutral_since]
                rearm = None
        else:
            rearm["neutral_since"] = None

    if event is None and rearm is None:
        candidate = _find_shock_candidate(parsed_history, sample, settings)
        if candidate is not None:
            anchor = candidate["anchor"]
            assert isinstance(anchor, PriceSample)
            direction = str(candidate["direction"])
            event = {
                "event_id": _event_id(str(state["session_date"]), direction, anchor.at),
                "direction": direction,
                "status": "shock_confirmed",
                "anchor_at": as_utc(anchor.at).isoformat(),
                "anchor_spx": anchor.spx,
                "anchor_es": anchor.es,
                "extreme_at": as_utc(sample.at).isoformat(),
                "extreme_es_at": as_utc(sample.at).isoformat(),
                "extreme_spx": sample.spx,
                "extreme_es": sample.es,
                "shock_spx_bps": float(candidate["spx_move_bps"]),
                "shock_es_bps": float(candidate["es_move_bps"]),
                "shock_threshold_bps": float(candidate["threshold_bps"]),
                "shock_duration_seconds": (sample.at - anchor.at).total_seconds(),
                "shock_delivered": False,
                "shock_last_attempt_at": None,
                "reclaim_streak": 0,
                "reclaim_confirmed_at": None,
                "reclaim_delivered": False,
                "reclaim_last_attempt_at": None,
                "reclaim_threshold": settings.reclaim_fraction,
                "reclaim_counted_spx_source_at": as_utc(
                    sample.spx_source_at or sample.at
                ).isoformat(),
                "reclaim_counted_es_source_at": as_utc(
                    sample.es_source_at or sample.at
                ).isoformat(),
                "spx_recovery_fraction": 0.0,
                "es_recovery_fraction": 0.0,
            }
    if event is not None:
        direction = str(event.get("direction"))
        extreme_spx = float(event["extreme_spx"])
        extreme_es = float(event["extreme_es"])
        spx_extension = (
            sample.spx < extreme_spx if direction == "down" else sample.spx > extreme_spx
        )
        es_extension = sample.es < extreme_es if direction == "down" else sample.es > extreme_es
        if spx_extension and event.get("reclaim_confirmed_at") is None:
            event["extreme_at"] = as_utc(sample.at).isoformat()
            event["extreme_spx"] = sample.spx
            event["reclaim_streak"] = 0
        if es_extension and event.get("reclaim_confirmed_at") is None:
            event["extreme_es_at"] = as_utc(sample.at).isoformat()
            event["extreme_es"] = sample.es
            event["reclaim_streak"] = 0

        anchor_spx = float(event["anchor_spx"])
        anchor_es = float(event["anchor_es"])
        extreme_spx = float(event["extreme_spx"])
        extreme_es = float(event["extreme_es"])
        extreme_spx_at = _event_datetime(event, "extreme_at") or sample.at
        extreme_es_at = _event_datetime(event, "extreme_es_at") or extreme_spx_at
        reclaim_anchor_at = max(extreme_spx_at, extreme_es_at)
        event["shock_spx_bps"] = _bps(extreme_spx, anchor_spx)
        event["shock_es_bps"] = _bps(extreme_es, anchor_es)
        anchor_at = _event_datetime(event, "anchor_at") or extreme_spx_at
        event["shock_duration_seconds"] = (extreme_spx_at - anchor_at).total_seconds()
        spx_recovery = _recovery_fraction(direction, sample.spx, anchor_spx, extreme_spx)
        es_recovery = _recovery_fraction(direction, sample.es, anchor_es, extreme_es)
        event["spx_recovery_fraction"] = spx_recovery
        event["es_recovery_fraction"] = es_recovery
        within_reclaim_window = (
            sample.at - reclaim_anchor_at
        ).total_seconds() <= settings.reclaim_window_seconds
        if event.get("reclaim_confirmed_at") is None:
            streak = int(event.get("reclaim_streak") or 0)
            counted_spx_at = _event_datetime(event, "reclaim_counted_spx_source_at")
            counted_es_at = _event_datetime(event, "reclaim_counted_es_source_at")
            sample_spx_at = as_utc(sample.spx_source_at or sample.at)
            sample_es_at = as_utc(sample.es_source_at or sample.at)
            fresh_pair = (
                counted_spx_at is None
                or counted_es_at is None
                or (sample_spx_at > counted_spx_at and sample_es_at > counted_es_at)
            )
            if fresh_pair:
                first_cross = (
                    within_reclaim_window
                    and spx_recovery >= settings.reclaim_fraction
                    and es_recovery >= settings.es_reclaim_fraction
                )
                hold_cross = (
                    within_reclaim_window
                    and spx_recovery >= settings.reclaim_hold_fraction
                    and es_recovery >= settings.es_reclaim_hold_fraction
                )
                if streak == 0:
                    streak = 1 if first_cross else 0
                else:
                    streak = streak + 1 if hold_cross else 0
                event["reclaim_streak"] = streak
                event["reclaim_counted_spx_source_at"] = sample_spx_at.isoformat()
                event["reclaim_counted_es_source_at"] = sample_es_at.isoformat()
                if streak >= settings.reclaim_confirm_samples:
                    event["status"] = "reclaim_confirmed"
                    event["reclaim_confirmed_at"] = as_utc(sample.at).isoformat()

    parsed_history.append(sample)
    state["samples"] = [item.to_dict() for item in parsed_history]
    state["active_event"] = event
    state["rearm"] = rearm
    state["updated_at"] = as_utc(sample.at).isoformat()

    alerts: list[Alert] = []
    if event is not None:
        if _pending_due(event, "shock", sample.at, settings.retry_seconds):
            alerts.append(_shock_alert(event))
        if event.get("reclaim_confirmed_at") and _pending_due(
            event, "reclaim", sample.at, settings.retry_seconds
        ):
            alerts.append(_reclaim_alert(event))
    return state, alerts


def mark_alert_attempts(
    state: dict[str, object], alerts: list[Alert], *, at: datetime, delivered: bool
) -> dict[str, object]:
    state = dict(state)
    strategy_event_ids = {
        str(alert.event_id) for alert in alerts if alert.kind in STRATEGY_KINDS and alert.event_id
    }
    if strategy_event_ids:
        state = mark_strategy_alert_attempts(
            state,
            event_ids=strategy_event_ids,
            at=at,
            delivered=delivered,
        )
    active = state.get("active_event")
    if not isinstance(active, dict):
        return state
    event = dict(active)
    for alert in alerts:
        if alert.event_id != event.get("event_id"):
            continue
        if alert.kind == SHOCK_KIND:
            phase = "shock"
        elif alert.kind == RECLAIM_KIND:
            phase = "reclaim"
        else:
            continue
        event[f"{phase}_last_attempt_at"] = as_utc(at).isoformat()
        if delivered:
            event[f"{phase}_delivered"] = True
            event[f"{phase}_delivered_at"] = as_utc(at).isoformat()
    state["active_event"] = event
    return state


def reconcile_acknowledged_alerts(
    state: dict[str, object],
    alerts: list[Alert],
    *,
    acknowledged_event_ids: set[str],
    at: datetime,
) -> tuple[dict[str, object], list[Alert]]:
    """Recover delivery after notifier state committed before monitor state."""

    recovered = [
        alert
        for alert in alerts
        if alert.dedup_group is not None and alert.dedup_group in acknowledged_event_ids
    ]
    if recovered:
        state = mark_alert_attempts(state, recovered, at=at, delivered=True)
    return state, [alert for alert in alerts if alert not in recovered]


def event_greek_shadow_due(state: dict[str, object], alert: Alert) -> bool:
    if alert.kind not in {SHOCK_KIND, RECLAIM_KIND} or not alert.event_id:
        return False
    phase = "shock" if alert.kind == SHOCK_KIND else "reclaim"
    for key in ("active_event", "last_event"):
        event = state.get(key)
        if isinstance(event, dict) and event.get("event_id") == alert.event_id:
            return not bool(event.get(f"{phase}_greeks_sampled_at"))
    return False


def mark_event_greek_shadow_sampled(
    state: dict[str, object],
    alerts: list[Alert],
    *,
    at: datetime,
) -> dict[str, object]:
    state = dict(state)
    for key in ("active_event", "last_event"):
        raw = state.get(key)
        if not isinstance(raw, dict):
            continue
        event = dict(raw)
        for alert in alerts:
            if event.get("event_id") != alert.event_id:
                continue
            if alert.kind == SHOCK_KIND:
                phase = "shock"
            elif alert.kind == RECLAIM_KIND:
                phase = "reclaim"
            else:
                continue
            event[f"{phase}_greeks_sampled_at"] = as_utc(at).isoformat()
        state[key] = event
    return state


def _quote_source_at(quote: Quote) -> datetime:
    return as_utc(quote.quote_time or quote.trade_time or quote.received_at)


def synchronized_live_sample(
    state: LatestState,
    settings: IntradayShockSettings,
) -> tuple[PriceSample | None, str | None]:
    spx = state.best_quote("index:SPX")
    es = state.best_quote("future:ES")
    if spx is None or es is None:
        return None, "missing_spx_or_es"
    if spx.provider != Provider.IBKR or es.provider != Provider.IBKR:
        return None, "non_ibkr_anchor"
    spx_decision = configured_quote_use_decision(spx, as_of=state.as_of)
    es_decision = configured_quote_use_decision(es, as_of=state.as_of)
    if (
        not spx_decision.alert_allowed
        or not es_decision.alert_allowed
        or spx_decision.feed_mode != MarketDataQuality.LIVE
        or es_decision.feed_mode != MarketDataQuality.LIVE
    ):
        return None, "non_live_or_stale_anchor"
    spx_price = spx.effective_price
    es_price = es.effective_price
    if spx_price is None or es_price is None or spx_price <= 0 or es_price <= 0:
        return None, "missing_anchor_price"
    spx_at = _quote_source_at(spx)
    es_at = _quote_source_at(es)
    if (as_utc(state.as_of) - spx_at).total_seconds() > settings.max_spx_age_seconds:
        return None, "stale_spx_anchor"
    if (as_utc(state.as_of) - es_at).total_seconds() > settings.max_es_age_seconds:
        return None, "stale_es_anchor"
    if abs((spx_at - es_at).total_seconds()) > settings.max_anchor_skew_seconds:
        return None, "anchor_timestamp_skew"
    return (
        PriceSample(
            at=max(spx_at, es_at),
            spx=float(spx_price),
            es=float(es_price),
            spx_source_at=spx_at,
            es_source_at=es_at,
        ),
        None,
    )


def rth_session_date(at: datetime) -> str | None:
    at_et = at.astimezone(NY_TZ)
    session = DEFAULT_MARKET_CALENDAR.session(at_et.date())
    if session is None or not (session.open_at <= at_et < session.close_at):
        return None
    return session.trading_date.isoformat()


def _notification_payload(
    state: LatestState,
    monitor_state: dict[str, object],
    alerts: list[Alert],
) -> dict[str, object]:
    return {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "as_of": state.as_of.isoformat(),
        "window": active_window(state.as_of).to_dict(now=state.as_of),
        "human_focus_context": {
            "prices": {
                "spx": state.best_quote("index:SPX").effective_price
                if state.best_quote("index:SPX")
                else None,
                "es": state.best_quote("future:ES").effective_price
                if state.best_quote("future:ES")
                else None,
            },
            "intraday_shock": monitor_state.get("active_event"),
            "conditional_call_bias": (
                monitor_state.get("call_strategy", {}).get("active_bias")
                if isinstance(monitor_state.get("call_strategy"), dict)
                else None
            ),
        },
        "alert_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the lightweight SPX/ES shock monitor.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--no-notify", action="store_true", help="Never send notifications.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = IntradayShockSettings.from_env()
    strategy_settings = IntradayStrategySettings.from_env()
    storage_settings = StorageSettings.from_env()
    data_platform_settings: DataPlatformSettings | None = None
    data_platform_config_error: str | None = None
    try:
        data_platform_settings = DataPlatformSettings.from_env()
    except Exception as exc:  # Optional research configuration is always fail-open.
        data_platform_config_error = f"{type(exc).__name__}:{exc}"
    latest = LatestStateStore(storage_settings).load()
    session_date = rth_session_date(latest.as_of)
    payload: dict[str, Any] = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "as_of": latest.as_of.isoformat(),
        "alert_count": 0,
        "alerts": [],
    }
    if session_date is None:
        payload["skipped_reason"] = "outside_spx_rth"
    else:
        sample, sample_error = synchronized_live_sample(latest, settings)
        if sample is None:
            payload["skipped_reason"] = sample_error
        else:
            options_map = None
            option_structure_error: str | None = None
            try:
                options_map = build_options_map(latest)
                structure = structure_from_options_map(
                    options_map,
                    session_date=session_date,
                    observed_at=sample.at,
                    state=latest,
                )
            except Exception as exc:  # Price alerts must survive option-map failures.
                option_structure_error = f"{type(exc).__name__}:{exc}"
                structure = unavailable_structure(
                    observed_at=sample.at,
                    reason="option_structure_build_error",
                )
            state_path = Path(settings.state_path)
            notify_settings = replace(
                NotificationSettings.from_env(),
                direct_push_llm_enabled=False,
            )
            with exclusive_state_lock(state_path):
                monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
                monitor_state, price_alerts = advance_monitor_state(monitor_state, sample, settings)
                monitor_state, path_decision, strategy_signals = advance_intraday_strategy(
                    monitor_state,
                    sample,
                    structure,
                    strategy_settings,
                )
                alerts = [*price_alerts, *(_strategy_alert(row) for row in strategy_signals)]
                raw_price_alerts = tuple(price_alerts)
                if alerts and not args.no_notify:
                    monitor_state, alerts = reconcile_acknowledged_alerts(
                        monitor_state,
                        alerts,
                        acknowledged_event_ids=set(
                            load_acknowledged_event_ids(notify_settings.state_path)
                        ),
                        at=sample.at,
                    )
                if alerts and not args.no_notify:
                    monitor_state = mark_alert_attempts(
                        monitor_state,
                        alerts,
                        at=sample.at,
                        delivered=False,
                    )
                atomic_write_json_secure(state_path, monitor_state)

            prepared_research = None
            research_result = None
            research_error = data_platform_config_error
            if data_platform_settings is not None and data_platform_settings.enabled:
                try:
                    # Pure ID/record preparation only. No research I/O occurs
                    # before the latency-critical notification attempt.
                    prepared_research = prepare_intraday_evaluation(
                        session_date=session_date,
                        source_at=sample.at,
                        available_at=latest.as_of,
                        spx=sample.spx,
                        es=sample.es,
                        spx_source_at=sample.spx_source_at or sample.at,
                        es_source_at=sample.es_source_at or sample.at,
                        structure=asdict(structure),
                        path_decision=path_decision.to_dict(),
                        alerts=tuple(alert.to_dict() for alert in alerts),
                        strategy_config=asdict(strategy_settings),
                        settings=data_platform_settings,
                    )
                    research_result = prepared_research.result
                except Exception as exc:  # Research preparation must never suppress an alert.
                    research_error = f"{type(exc).__name__}:{exc}"
            elif data_platform_settings is not None:
                research_result = IntradayResearchResult(status="disabled")

            payload = _notification_payload(latest, monitor_state, alerts)
            payload["intraday_path"] = path_decision.to_dict()
            research_link_by_alert: dict[tuple[str, str], object] = {}
            if research_result is not None:
                alert_rows = payload.get("alerts")
                if isinstance(alert_rows, list):
                    for alert, row, link in zip(
                        alerts,
                        alert_rows,
                        research_result.alert_links,
                        strict=False,
                    ):
                        if isinstance(row, dict):
                            row["source_at"] = link.source_at.isoformat()
                            row["event_key"] = link.event_key
                            row["decision_id"] = link.decision_id
                        research_link_by_alert[(alert.kind, str(alert.event_id or ""))] = link
                payload["data_platform"] = {
                    "status": research_result.status,
                    "evaluation_event_key": research_result.evaluation_event_key,
                    "evaluation_decision_id": research_result.evaluation_decision_id,
                    "alert_link_count": len(research_result.alert_links),
                    "errors": list(research_result.errors),
                }
            elif research_error is not None:
                payload["data_platform"] = {"status": "error", "error": research_error}
            if option_structure_error is not None:
                payload["option_structure_error"] = option_structure_error

            # Delivery stays on the latency-critical path. Outcome and Greeks
            # telemetry run only after the deterministic alert attempt.
            notification_result = None
            if alerts and not args.no_notify:
                result = notify_payload(
                    payload,
                    settings=notify_settings,
                    now=sample.at,
                    record_telemetry=False,
                )
                notification_result = result
                payload["notification"] = result.to_dict()
                acknowledged = set(result.acknowledged_event_ids)
                delivered_alerts = [
                    alert
                    for alert in alerts
                    if alert.dedup_group is not None
                    and str(alert.dedup_group) in acknowledged
                ]
                if delivered_alerts:
                    with exclusive_state_lock(state_path):
                        latest_monitor_state = load_monitor_state(
                            settings.state_path,
                            session_date=session_date,
                        )
                        latest_monitor_state = mark_alert_attempts(
                            latest_monitor_state,
                            delivered_alerts,
                            at=sample.at,
                            delivered=True,
                        )
                        atomic_write_json_secure(state_path, latest_monitor_state)

            # Research persistence is deliberately after notification and its
            # durable delivery acknowledgement. It may spool, but cannot add
            # latency to the user-visible alert.
            if prepared_research is not None and data_platform_settings is not None:
                try:
                    research_result = persist_intraday_evaluation(
                        prepared_research,
                        settings=data_platform_settings,
                    )
                    payload["data_platform"] = {
                        "status": research_result.status,
                        "evaluation_event_key": research_result.evaluation_event_key,
                        "evaluation_decision_id": research_result.evaluation_decision_id,
                        "alert_link_count": len(research_result.alert_links),
                        "errors": list(research_result.errors),
                    }
                    if notification_result is not None:
                        selected_keys = set(notification_result.selected_alert_keys)
                        alert_rows = payload.get("alerts")
                        selected_rows = tuple(
                            row
                            for row in alert_rows
                            if isinstance(row, dict)
                            and str(row.get("decision_id") or alert_key(row)) in selected_keys
                        ) if isinstance(alert_rows, list) else ()
                        record_notification_result(
                            payload=payload,
                            selected_alerts=selected_rows,
                            notification=notification_result.to_dict(),
                            attempted_at=sample.at,
                            settings=data_platform_settings,
                        )
                except Exception as exc:  # Research storage must never suppress an alert.
                    payload["data_platform"] = {
                        "status": "error",
                        "error": f"{type(exc).__name__}:{exc}",
                    }

            outcome_summary: dict[str, object] = {"status": "ok", "records_emitted": 0}
            try:
                tracker = IntradayEventOutcomeTracker(IntradayEventOutcomeSettings.from_env())
                outcome_sample = SynchronizedSPXSample(
                    spx=sample.spx,
                    spx_source_at=sample.spx_source_at or sample.at,
                    es_source_at=sample.es_source_at or sample.at,
                )
                outcome_alerts = (
                    *raw_price_alerts,
                    *(alert for alert in alerts if alert.kind in STRATEGY_KINDS),
                )
                active_event = monitor_state.get("active_event")
                market_direction = (
                    str(active_event.get("direction"))
                    if isinstance(active_event, dict)
                    else "down"
                )
                for alert in outcome_alerts:
                    if not alert.event_id:
                        continue
                    if alert.kind == SHOCK_KIND:
                        phase = "shock"
                        direction = market_direction
                    elif alert.kind == RECLAIM_KIND:
                        phase = "reclaim"
                        direction = market_direction
                    else:
                        phase = "strategy"
                        direction = "up"
                    research_link = research_link_by_alert.get(
                        (alert.kind, str(alert.event_id))
                    )
                    if direction in {"up", "down"}:
                        tracker.observe_event(
                            event_id=str(alert.event_id),
                            phase=phase,
                            direction=direction,
                            sample=outcome_sample,
                            event_key=getattr(research_link, "event_key", None),
                            decision_id=getattr(research_link, "decision_id", None),
                        )
                emitted = tracker.observe_sample(outcome_sample)
                outcome_summary["records_emitted"] = len(emitted)
                if data_platform_settings is not None:
                    outcome_summary["ledger_records"] = len(
                        record_outcome_rows(
                            emitted,
                            settings=data_platform_settings,
                        )
                    )
            except Exception as exc:  # Outcome telemetry must never suppress an alert.
                outcome_summary = {
                    "status": "error",
                    "error": f"{type(exc).__name__}:{exc}",
                }
            payload["outcome_tracking"] = outcome_summary

            greek_event_results: list[dict[str, object]] = []
            sampled_alerts: list[Alert] = []
            for alert in raw_price_alerts:
                if not event_greek_shadow_due(monitor_state, alert):
                    continue
                trigger_kind = "shock" if alert.kind == SHOCK_KIND else "reclaim"
                research_link = research_link_by_alert.get(
                    (alert.kind, str(alert.event_id or ""))
                )
                result = sample_zero_dte_greeks_shadow(
                    latest,
                    data_root=storage_settings.data_root,
                    trigger_kind=trigger_kind,
                    event_id=str(alert.event_id),
                    event_at=sample.at,
                    trigger_metadata={
                        "direction": (
                            str(monitor_state.get("active_event", {}).get("direction"))
                            if isinstance(monitor_state.get("active_event"), dict)
                            else None
                        ),
                        "spx": sample.spx,
                        "es": sample.es,
                        "event_key": getattr(research_link, "event_key", None),
                    },
                    options_map=options_map,
                )
                greek_event_results.append(result.to_dict())
                if result.status != "error":
                    sampled_alerts.append(alert)
            if greek_event_results:
                payload["greek_shadow_events"] = greek_event_results
            if sampled_alerts:
                with exclusive_state_lock(state_path):
                    latest_monitor_state = load_monitor_state(
                        settings.state_path,
                        session_date=session_date,
                    )
                    latest_monitor_state = mark_event_greek_shadow_sampled(
                        latest_monitor_state,
                        sampled_alerts,
                        at=sample.at,
                    )
                    atomic_write_json_secure(state_path, latest_monitor_state)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    elif payload.get("skipped_reason"):
        print(f"Intraday shock monitor skipped: {payload['skipped_reason']}")
    else:
        print(f"Intraday shock alerts: {payload['alert_count']}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
