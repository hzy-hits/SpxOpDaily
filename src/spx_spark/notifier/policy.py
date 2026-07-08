from __future__ import annotations

import re


SEVERITY_RANK = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

POSITIVE_DELIVERY_CUES = (
    "需要看盘",
    "需要人类",
    "需要关注",
    "需要立即",
    "高风险",
)

NEGATIVE_DELIVERY_CUES = (
    "不需要推送",
    "无需推送",
    "不要推送",
    "不推送",
    "不需要看盘",
    "无需看盘",
)

HUMAN_VISIBLE_ALERT_PREFIXES = (
    "index:SPX",
    "future:ES",
    "option:SPX:SPXW",
    "option_map:SPXW",
    "iv_surface:SPXW",
)

# Scope gate policy: keep the focus rules in the prompt, and hard-block only
# context sources the human never trades against (crypto proxies, prediction
# markets). Symbol mentions like SPY/QQQ/VIX are allowed as confirmation and
# vol-regime context; blocking whole messages for them proved too strict and
# silently dropped useful analyses.
BLOCKED_HUMAN_MESSAGE_SYMBOLS: tuple[str, ...] = ()

BLOCKED_HUMAN_MESSAGE_PHRASES = (
    "hyperliquid",
    "polymarket",
    "crypto_perp",
    "prediction market",
)

SYSTEM_EVENT_ALERT_KINDS = {
    "ibkr_session_interrupted",
    "ibkr_session_restored",
}

POSITION_HOLDING_ALERT_KIND_PREFIX = "spxw_position_"
POSITION_HOLDING_SOURCE_GATE = "ibkr_positions"
POSITION_DIRECT_PUSH_KINDS = frozenset(
    {
        "spxw_position_opened",
        "spxw_position_closed",
        "spxw_position_qty_changed",
        "spxw_position_book_pnl",
    }
)

# IV-surface movement alerts (put skew steepening, ATM IV jumps, surface
# shifts) intentionally go through the agent review path instead of direct
# push: raw single-metric pushes were too noisy and carried no gamma/VIX
# context. Only position events and IBKR session events bypass review.


def severity_value(value: object) -> int:
    return SEVERITY_RANK.get(str(value or "").lower(), -1)


def alert_key(alert: dict[str, object]) -> str:
    dedup_group = alert.get("dedup_group")
    return "|".join(
        (
            str(alert.get("kind") or ""),
            str(alert.get("instrument_id") or ""),
            "" if dedup_group is None else str(dedup_group),
        )
    )


def is_human_visible_alert(alert: dict[str, object]) -> bool:
    if alert.get("research_only") is True:
        return False
    kind = str(alert.get("kind") or "").lower()
    source_gate = str(alert.get("source_gate") or "").lower()
    blocked_terms = ("smart", "wallet", "onchain", "hyperliquid_proxy")
    if any(term in kind or term in source_gate for term in blocked_terms):
        return False
    instrument_id = str(alert.get("instrument_id") or "")
    return any(instrument_id.startswith(prefix) for prefix in HUMAN_VISIBLE_ALERT_PREFIXES)


def is_system_event_alert(alert: dict[str, object]) -> bool:
    return str(alert.get("kind") or "") in SYSTEM_EVENT_ALERT_KINDS


def is_position_holding_alert(alert: dict[str, object]) -> bool:
    kind = str(alert.get("kind") or "")
    if kind not in POSITION_DIRECT_PUSH_KINDS:
        return False
    return str(alert.get("source_gate") or "") == POSITION_HOLDING_SOURCE_GATE


def direct_push_alerts(alerts: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        alert
        for alert in alerts
        if is_system_event_alert(alert) or is_position_holding_alert(alert)
    ]


# Friend Bark channel: pure market signals only. Ops/engineering kinds (data
# degradation, session drops, freshness gates) and the user's private position
# alerts stay off this list on purpose.
MARKET_SIGNAL_ALERT_KINDS = frozenset(
    {
        "price_move_from_close",
        "option_gamma_regime",
        "option_wall_proximity",
        "iv_term_gap",
        "atm_iv_jump_5m",
        "put_skew_steepening_5m",
        "iv_surface_shift_5m",
        "iv_surface_shift_1h",
        "atm_iv_change_1h",
    }
)


def is_market_signal_alert(alert: dict[str, object]) -> bool:
    return str(alert.get("kind") or "") in MARKET_SIGNAL_ALERT_KINDS


def alerts_are_market_signals(alerts: list[dict[str, object]]) -> bool:
    """True when every alert in the batch is a market signal (no ops noise)."""
    return bool(alerts) and all(is_market_signal_alert(alert) for alert in alerts)


def codex_message_requests_delivery(message: str) -> bool:
    normalized = message.strip().lower()
    if any(cue in normalized for cue in NEGATIVE_DELIVERY_CUES):
        return False
    first_line = normalized.splitlines()[0] if normalized else ""
    return any(first_line.startswith(cue) for cue in POSITIVE_DELIVERY_CUES)


def codex_message_respects_human_scope(message: str) -> bool:
    lowered = message.lower()
    if any(phrase in lowered for phrase in BLOCKED_HUMAN_MESSAGE_PHRASES):
        return False
    uppered = message.upper()
    return not any(
        re.search(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", uppered)
        for symbol in BLOCKED_HUMAN_MESSAGE_SYMBOLS
    )
