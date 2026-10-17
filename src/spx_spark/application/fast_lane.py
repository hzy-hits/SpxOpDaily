"""1s fast lane: publish watch/armed from the latest committed levels.

The 5s Core cycle owns chain analysis and authorization; this lane only answers
"is the committed setup being approached or triggered right now". It never
authorizes and never sends a ready card.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from spx_spark.clock import Clock, SystemClock
from spx_spark.config import StorageSettings
from spx_spark.marketdata import Quote
from spx_spark.state_io import atomic_write_json_secure, read_json_object
from spx_spark.storage import LatestState, LatestStateStore

FAST_LANE_INTERVAL_SECONDS = 1.0
READY_BUDGET_MS = 3000.0
FAST_LANE_PATH = "latest/fast_lane.json"


def run_fast_lane_cycle(
    *,
    storage_settings: StorageSettings,
    state: LatestState | None = None,
    decision: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    clock: Clock | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    clock = clock or SystemClock()
    now = now or clock.now()
    started = clock.monotonic()
    root = Path(storage_settings.data_root)
    state = state or LatestStateStore(storage_settings).load()
    decision = decision if decision is not None else read_json_object(
        root / "latest" / "strategy_decision.json"
    )
    quote = _spot_quote(state)
    candidate = decision.get("candidate") if isinstance(decision.get("candidate"), Mapping) else {}
    direction = str(candidate.get("direction") or "").upper()
    trigger = _number(candidate.get("trigger_level"))
    spot = _price(quote)
    valid_until = _time(candidate.get("opportunity_valid_until"))
    state_name = "none"
    reason = "no_candidate"
    if candidate and spot is None:
        reason = "spot_unavailable"
    elif valid_until is not None and valid_until <= now:
        state_name = "expired"
        reason = "opportunity_expired"
    elif candidate and trigger is not None and direction in {"UP", "BULL", "LONG"}:
        state_name = "armed" if spot >= trigger else "watch"
        reason = "trigger_crossed" if state_name == "armed" else "waiting_for_trigger"
    elif candidate and trigger is not None and direction in {"DOWN", "BEAR", "SHORT"}:
        state_name = "armed" if spot <= trigger else "watch"
        reason = "trigger_crossed" if state_name == "armed" else "waiting_for_trigger"
    elif candidate:
        state_name = "watch"
        reason = "no_trigger_level"
    payload = {
        "schema_version": 1,
        "lane": "fast",
        "state": state_name,
        "reason": reason,
        "as_of": now.astimezone(timezone.utc).isoformat(),
        "decision_id": decision.get("decision_id"),
        "direction": direction or None,
        "trigger_level": trigger,
        "spot": spot,
        "evidence_age_ms": _age_ms(quote, now),
        "decision_age_ms": _decision_age_ms(decision, now),
        "valid_until": valid_until.isoformat() if valid_until else None,
        "latency_ms": round((clock.monotonic() - started) * 1000.0, 3),
        "fast_interval_ms": FAST_LANE_INTERVAL_SECONDS * 1000.0,
        "ready_budget_ms": READY_BUDGET_MS,
        "authority": "none",
    }
    if persist:
        atomic_write_json_secure(root / FAST_LANE_PATH, payload)
    return payload


def run_periodic(storage_settings: StorageSettings) -> int:
    run_fast_lane_cycle(storage_settings=storage_settings)
    return 0


def _spot_quote(state: LatestState) -> Quote | None:
    for instrument_id in ("index:SPX", "equity:SPY", "future:ES"):
        quote = state.best_quote(instrument_id)
        if quote is not None and _price(quote) is not None:
            return quote
    return None


def _price(quote: Quote | None) -> float | None:
    if quote is None:
        return None
    for value in (quote.last, quote.mid, quote.mark, quote.close):
        if value is not None and value > 0:
            return float(value)
    return None


def _age_ms(quote: Quote | None, now: datetime) -> float | None:
    if quote is None:
        return None
    received = quote.received_at
    if received.tzinfo is None:
        received = received.replace(tzinfo=timezone.utc)
    return round((now - received.astimezone(timezone.utc)).total_seconds() * 1000.0, 3)


def _decision_age_ms(decision: Mapping[str, Any], now: datetime) -> float | None:
    decided = _time(decision.get("decision_at"))
    if decided is None:
        return None
    return round((now - decided).total_seconds() * 1000.0, 3)


def _time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
