"""Shock / reclaim state machine and alert builders."""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from spx_spark.alert_model import Alert
from spx_spark.application.shock.models import (
    NET_PREMIUM_BEARISH_DIVERGENCE_KIND,
    RECLAIM_KIND,
    SHOCK_KIND,
    IntradayShockSettings,
    PriceSample,
    _parse_datetime,
    _sample_from_dict,
)
from spx_spark.data_platform.ids import deterministic_id
from spx_spark.intraday_strategy import (
    FLIP_RECLAIM_CALL_KIND,
    IntradayPathSignal,
)
from spx_spark.marketdata import (
    InstrumentType,
    MarketDataQuality,
    OptionRight,
    Provider,
    Quote,
    as_utc,
)


_ET = ZoneInfo("America/New_York")
_NET_PREMIUM_POLICY_VERSION = "captured_net_premium_bearish_divergence.v1"
_NET_PREMIUM_LOOKBACK_MINUTES = 15
_NET_PREMIUM_CONFIRM_MINUTES = 5
_NET_PREMIUM_COOLDOWN_SECONDS = 15 * 60
_NET_PREMIUM_MIN_COVERAGE = 0.10
_NET_PREMIUM_MAX_QUOTE_AGE_SECONDS = 15.0
_NET_PREMIUM_MAX_TRADE_LAG_SECONDS = 5.0
_NET_PREMIUM_HISTORY_MINUTES = 30
_NET_PREMIUM_SEEN_RETENTION_MINUTES = 45
_NET_PREMIUM_START_ET = time(10, 0)
_NET_PREMIUM_END_ET = time(15, 30)


def _minute_start(value: datetime) -> datetime:
    return as_utc(value).replace(second=0, microsecond=0)


def _option_expiry_matches_session(quote: Quote, session_date: str) -> bool:
    expiry = str(quote.instrument.expiry or "").replace("-", "")
    return expiry == session_date.replace("-", "")


def _fresh_schwab_zero_dte_options(
    quotes: tuple[Quote, ...],
    *,
    decision_at: datetime,
    session_date: str,
) -> tuple[Quote, ...]:
    latest_by_instrument: dict[str, Quote] = {}
    for quote in quotes:
        if (
            quote.provider is not Provider.SCHWAB
            or quote.quality is not MarketDataQuality.LIVE
            or quote.sampling_mode != "schwab_stream"
            or quote.instrument.instrument_type is not InstrumentType.OPTION
            or (quote.instrument.underlier or quote.instrument.symbol) != "SPX"
            or quote.instrument.right not in {OptionRight.CALL, OptionRight.PUT}
            or not _option_expiry_matches_session(quote, session_date)
        ):
            continue
        received_at = as_utc(quote.received_at)
        age = (decision_at - received_at).total_seconds()
        if age < -5.0 or age > _NET_PREMIUM_MAX_QUOTE_AGE_SECONDS:
            continue
        instrument_id = quote.instrument.canonical_id
        previous = latest_by_instrument.get(instrument_id)
        if previous is None or as_utc(previous.received_at) < received_at:
            latest_by_instrument[instrument_id] = quote
    return tuple(latest_by_instrument.values())


def _net_premium_bucket(
    minutes: dict[str, dict[str, object]], minute_at: datetime
) -> dict[str, object]:
    key = _minute_start(minute_at).isoformat()
    raw = minutes.get(key)
    bucket = dict(raw) if isinstance(raw, dict) else {}
    bucket.setdefault("minute_at", key)
    bucket.setdefault("call_net", 0.0)
    bucket.setdefault("put_net", 0.0)
    bucket.setdefault("classified_size", 0.0)
    bucket.setdefault("captured_size", 0.0)
    bucket.setdefault("volume_delta", 0.0)
    minutes[key] = bucket
    return bucket


def _last_trade_fingerprint(quote: Quote) -> str | None:
    if (
        quote.trade_time is None
        or quote.last is None
        or quote.last <= 0
        or quote.last_size is None
        or quote.last_size <= 0
    ):
        return None
    return "|".join(
        (
            as_utc(quote.trade_time).isoformat(),
            f"{float(quote.last):.10g}",
            f"{float(quote.last_size):.10g}",
        )
    )


def _format_premium_dollars(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{sign}${absolute / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute / 1_000:.0f}K"
    return f"{sign}${absolute:.0f}"


def _bearish_divergence_alert(
    *,
    session_date: str,
    decision_at: datetime,
    signal_minute: datetime,
    current_spx: float,
    recent_high: float,
    call_net: float,
    put_net: float,
    coverage: float,
    classified_size: float,
    volume_delta: float,
) -> Alert:
    stamp = as_utc(signal_minute).strftime("%H%M")
    event_id = (
        "spx_net_premium_bearish_divergence:"
        f"{session_date.replace('-', '')}:{stamp}"
    )
    return Alert(
        severity="high",
        kind=NET_PREMIUM_BEARISH_DIVERGENCE_KIND,
        instrument_id="index:SPX",
        title="SPX 熊背离观察：局部新高失败，Put 净权利金占优",
        detail=(
            f"SPX 近 5 分钟冲出 {recent_high:.1f} 后回落至 {current_spx:.1f}；"
            "捕获的 0DTE 成交代理显示 "
            f"Call 净权利金 {_format_premium_dollars(call_net)}、"
            f"Put 净权利金 {_format_premium_dollars(put_net)}，"
            f"可分类成交量覆盖约 {coverage:.1%}。"
            "这是前向未验证的熊背离观察，不是做空或 Put 入场授权；等待价格结构确认。"
        ),
        provider=Provider.SCHWAB.value,
        quality=MarketDataQuality.LIVE.value,
        value=current_spx,
        threshold=recent_high,
        research_only=False,
        source_gate="captured_net_premium_proxy_bearish_divergence_v1",
        dedup_group=f"{event_id}:observe",
        event_id=event_id,
        source_at=as_utc(decision_at).isoformat(),
        cooldown_seconds=float(_NET_PREMIUM_COOLDOWN_SECONDS),
        audit_context={
            "policy_version": _NET_PREMIUM_POLICY_VERSION,
            "authority": "observe_only",
            "execution_eligible": False,
            "automatic_ordering": False,
            "signal_minute": as_utc(signal_minute).isoformat(),
            "call_net_premium_dollars": call_net,
            "put_net_premium_dollars": put_net,
            "at_touch_volume_coverage": coverage,
            "classified_size": classified_size,
            "cumulative_volume_delta": volume_delta,
            "classification": "last_at_ask_buy_last_at_bid_sell_inside_unknown",
            "known_limit": "schwab_l1_captured_last_trade_proxy_not_full_opra_tape",
        },
    )


def advance_net_premium_bearish_divergence(
    state: dict[str, object],
    sample: PriceSample,
    *,
    quotes: tuple[Quote, ...],
    decision_at: datetime,
    session_date: str,
) -> tuple[dict[str, object], list[Alert]]:
    """Advance the causal captured-tape proxy and emit observe-only bearish divergence."""

    state = dict(state)
    decision_at = as_utc(decision_at)
    raw_tape = state.get("captured_net_premium_divergence")
    tape = dict(raw_tape) if isinstance(raw_tape, dict) else {}
    raw_minutes = tape.get("minutes")
    minutes = {
        str(key): dict(value)
        for key, value in raw_minutes.items()
        if isinstance(raw_minutes, dict) and isinstance(key, str) and isinstance(value, dict)
    } if isinstance(raw_minutes, dict) else {}
    raw_seen = tape.get("seen_options")
    seen = {
        str(key): dict(value)
        for key, value in raw_seen.items()
        if isinstance(raw_seen, dict) and isinstance(key, str) and isinstance(value, dict)
    } if isinstance(raw_seen, dict) else {}

    current_bucket = _net_premium_bucket(minutes, sample.at)
    current_bucket["spx"] = float(sample.spx)
    current_bucket["spx_at"] = as_utc(sample.spx_source_at or sample.at).isoformat()

    cumulative_classified_size = float(tape.get("cumulative_classified_size") or 0.0)
    cumulative_volume_delta = float(tape.get("cumulative_volume_delta") or 0.0)
    for quote in _fresh_schwab_zero_dte_options(
        quotes,
        decision_at=decision_at,
        session_date=session_date,
    ):
        instrument_id = quote.instrument.canonical_id
        prior_raw = seen.get(instrument_id)
        prior = dict(prior_raw) if isinstance(prior_raw, dict) else None
        received_at = as_utc(quote.received_at)
        received_iso = received_at.isoformat()
        if prior is not None and prior.get("received_at") == received_iso:
            continue

        volume = (
            float(quote.volume)
            if isinstance(quote.volume, int | float) and quote.volume >= 0
            else None
        )
        prior_volume_raw = prior.get("volume") if prior is not None else None
        prior_volume = (
            float(prior_volume_raw)
            if isinstance(prior_volume_raw, int | float) and prior_volume_raw >= 0
            else None
        )
        volume_delta = (
            max(volume - prior_volume, 0.0)
            if volume is not None and prior_volume is not None
            else 0.0
        )
        fingerprint = _last_trade_fingerprint(quote)
        previous_fingerprint = str(prior.get("fingerprint") or "") if prior else ""
        seen[instrument_id] = {
            "received_at": received_iso,
            "volume": volume,
            "fingerprint": fingerprint,
        }
        bucket = _net_premium_bucket(minutes, received_at)
        if volume_delta > 0:
            bucket["volume_delta"] = float(bucket["volume_delta"]) + volume_delta
            cumulative_volume_delta += volume_delta

        if prior is None or fingerprint is None or fingerprint == previous_fingerprint:
            continue
        trade_at = as_utc(quote.trade_time) if quote.trade_time is not None else None
        trade_lag = (
            (received_at - trade_at).total_seconds() if trade_at is not None else math.inf
        )
        if (
            trade_lag < 0
            or trade_lag > _NET_PREMIUM_MAX_TRADE_LAG_SECONDS
            or quote.bid is None
            or quote.ask is None
            or quote.last is None
            or quote.last_size is None
            or quote.bid < 0
            or quote.ask <= quote.bid
            or quote.last <= 0
            or quote.last_size <= 0
        ):
            continue
        bucket["captured_size"] = float(bucket["captured_size"]) + float(quote.last_size)
        inferred_side = 1 if quote.last >= quote.ask else -1 if quote.last <= quote.bid else 0
        if inferred_side == 0:
            continue
        premium = float(quote.last) * float(quote.last_size) * 100.0
        flow_key = "call_net" if quote.instrument.right is OptionRight.CALL else "put_net"
        bucket[flow_key] = float(bucket[flow_key]) + inferred_side * premium
        bucket["classified_size"] = float(bucket["classified_size"]) + float(
            quote.last_size
        )
        cumulative_classified_size += float(quote.last_size)

    history_cutoff = decision_at - timedelta(minutes=_NET_PREMIUM_HISTORY_MINUTES)
    minutes = {
        key: value
        for key, value in minutes.items()
        if (parsed := _parse_datetime(key)) is not None and parsed >= history_cutoff
    }
    seen_cutoff = decision_at - timedelta(minutes=_NET_PREMIUM_SEEN_RETENTION_MINUTES)
    seen = {
        key: value
        for key, value in seen.items()
        if (parsed := _parse_datetime(value.get("received_at"))) is not None
        and parsed >= seen_cutoff
    }
    tape["minutes"] = minutes
    tape["seen_options"] = seen
    tape["cumulative_classified_size"] = cumulative_classified_size
    tape["cumulative_volume_delta"] = cumulative_volume_delta
    tape["updated_at"] = decision_at.isoformat()

    completed_minute = _minute_start(decision_at) - timedelta(minutes=1)
    last_evaluated = _parse_datetime(tape.get("last_evaluated_minute"))
    if last_evaluated is not None and last_evaluated >= completed_minute:
        state["captured_net_premium_divergence"] = tape
        return state, []
    tape["last_evaluated_minute"] = completed_minute.isoformat()

    rows: list[tuple[datetime, dict[str, object]]] = []
    for key, bucket in minutes.items():
        minute_at = _parse_datetime(key)
        if (
            minute_at is not None
            and minute_at <= completed_minute
            and isinstance(bucket.get("spx"), int | float)
        ):
            rows.append((minute_at, bucket))
    rows.sort(key=lambda item: item[0])
    required = _NET_PREMIUM_LOOKBACK_MINUTES + _NET_PREMIUM_CONFIRM_MINUTES
    window = rows[-required:]
    signal_time_et = completed_minute.astimezone(_ET).time()
    coverage = (
        cumulative_classified_size / cumulative_volume_delta
        if cumulative_volume_delta > 0
        else 0.0
    )
    last_alert_at = _parse_datetime(tape.get("last_alert_at"))
    cooldown_active = (
        last_alert_at is not None
        and (decision_at - last_alert_at).total_seconds() < _NET_PREMIUM_COOLDOWN_SECONDS
    )
    contiguous = len(window) == required and all(
        (window[index][0] - window[index - 1][0]).total_seconds() == 60
        for index in range(1, len(window))
    )
    if (
        not contiguous
        or not (_NET_PREMIUM_START_ET <= signal_time_et <= _NET_PREMIUM_END_ET)
        or coverage < _NET_PREMIUM_MIN_COVERAGE
        or cooldown_active
    ):
        tape["coverage"] = coverage
        state["captured_net_premium_divergence"] = tape
        return state, []

    prior = window[:_NET_PREMIUM_LOOKBACK_MINUTES]
    recent = window[_NET_PREMIUM_LOOKBACK_MINUTES:]
    prior_high = max(float(bucket["spx"]) for _, bucket in prior)
    recent_high = max(float(bucket["spx"]) for _, bucket in recent)
    current_spx = float(recent[-1][1]["spx"])
    call_net = sum(float(bucket.get("call_net") or 0.0) for _, bucket in recent)
    put_net = sum(float(bucket.get("put_net") or 0.0) for _, bucket in recent)
    if not (recent_high > prior_high and current_spx < recent_high and call_net <= 0 < put_net):
        tape["coverage"] = coverage
        state["captured_net_premium_divergence"] = tape
        return state, []

    alert = _bearish_divergence_alert(
        session_date=session_date,
        decision_at=decision_at,
        signal_minute=completed_minute,
        current_spx=current_spx,
        recent_high=recent_high,
        call_net=call_net,
        put_net=put_net,
        coverage=coverage,
        classified_size=cumulative_classified_size,
        volume_delta=cumulative_volume_delta,
    )
    tape["coverage"] = coverage
    tape["last_alert_at"] = decision_at.isoformat()
    tape["last_alert_event_id"] = alert.event_id
    state["captured_net_premium_divergence"] = tape
    return state, [alert]

def _event_datetime(event: dict[str, object], field: str) -> datetime | None:
    return _parse_datetime(event.get(field))


def _bps(current: float, anchor: float) -> float:
    return (current / anchor - 1.0) * 10_000.0


def _event_id(
    session_date: str,
    direction: str,
    anchor_at: datetime,
    *,
    provider: str = Provider.UNKNOWN.value,
) -> str:
    stamp = as_utc(anchor_at).strftime("%H%M%S")
    base = f"spx_shock:{session_date.replace('-', '')}:{direction}:{stamp}"
    return base if provider == Provider.UNKNOWN.value else f"{base}:{provider}"


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
        if sample.provider == current.provider
        and 0 < (current.at - sample.at).total_seconds() <= horizon_seconds
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


def _provider_switch_reset_due(
    container: dict[str, object],
    sample: PriceSample,
    *,
    reset_seconds: int,
) -> bool:
    expected = str(container.get("provider") or Provider.UNKNOWN.value)
    if expected == Provider.UNKNOWN.value:
        container["provider"] = sample.provider
        container.pop("provider_mismatch_since", None)
        return False
    if expected == sample.provider:
        container.pop("provider_mismatch_since", None)
        return False
    mismatch_since = _event_datetime(container, "provider_mismatch_since")
    if mismatch_since is None:
        container["provider_mismatch_since"] = as_utc(sample.at).isoformat()
        return False
    return (sample.at - mismatch_since).total_seconds() >= reset_seconds


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
        provider=str(event.get("provider") or Provider.UNKNOWN.value),
        quality=MarketDataQuality.LIVE.value,
        value=spx_move,
        threshold=float(event["shock_threshold_bps"]),
        research_only=True,
        source_gate="spx_es_intraday_shock_audit",
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
        provider=str(event.get("provider") or Provider.UNKNOWN.value),
        quality=MarketDataQuality.LIVE.value,
        value=spx_recovery,
        threshold=float(event["reclaim_threshold"]),
        research_only=True,
        source_gate="spx_es_intraday_reclaim_audit",
        dedup_group=f"{event_id}:reclaim",
        event_id=event_id,
        source_at=str(event.get("reclaim_confirmed_at") or event.get("extreme_at") or "")
        or None,
    )


def _strategy_alert(signal: IntradayPathSignal, *, provider: str) -> Alert:
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
        provider=provider,
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

    if event is not None and _provider_switch_reset_due(
        event,
        sample,
        reset_seconds=settings.provider_switch_reset_seconds,
    ):
        event["status"] = "provider_switched"
        event["provider_switched_at"] = as_utc(sample.at).isoformat()
        state["last_event"] = event
        event = None
        rearm = None
        parsed_history = [prior for prior in parsed_history if prior.provider == sample.provider]
    elif event is None and rearm is not None and _provider_switch_reset_due(
        rearm,
        sample,
        reset_seconds=settings.provider_switch_reset_seconds,
    ):
        rearm = None
        parsed_history = [prior for prior in parsed_history if prior.provider == sample.provider]

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
                "provider": event.get("provider"),
                "neutral_since": None,
            }
            event = None

    if event is None and rearm is not None:
        rearm_provider = str(rearm.get("provider") or Provider.UNKNOWN.value)
        if rearm_provider in {Provider.UNKNOWN.value, sample.provider}:
            direction = str(rearm.get("direction"))
            anchor_spx = float(rearm.get("anchor_spx") or sample.spx)
            extreme_spx = float(rearm.get("extreme_spx") or sample.spx)
            recovery = _recovery_fraction(direction, sample.spx, anchor_spx, extreme_spx)
            neutral_since = _event_datetime(rearm, "neutral_since")
            if recovery >= settings.rearm_recovery_fraction:
                if neutral_since is None:
                    rearm["neutral_since"] = as_utc(sample.at).isoformat()
                elif (
                    sample.at - neutral_since
                ).total_seconds() >= settings.rearm_neutral_seconds:
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
                "event_id": _event_id(
                    str(state["session_date"]),
                    direction,
                    anchor.at,
                    provider=sample.provider,
                ),
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
                "provider": sample.provider,
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
    event_provider = (
        str(event.get("provider") or Provider.UNKNOWN.value)
        if event is not None
        else Provider.UNKNOWN.value
    )
    if event is not None and event_provider in {Provider.UNKNOWN.value, sample.provider}:
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
