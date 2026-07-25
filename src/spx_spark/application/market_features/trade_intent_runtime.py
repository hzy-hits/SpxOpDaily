"""Persistence and human delivery for deterministic trade-ready intents."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.pricing import round_to_tick
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestStateStore, configured_quote_use_decision
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
)


DELIVERY_LEASE_SECONDS = 120.0


TRADE_INTENT_SYSTEM_PROMPT = """你是 SPX 指数期权自营台的 execution trader，只负责排版一条已经通过代码硬门槛的 0DTE 交易意图。
写成机构级 execution ticket，不是散户喊单、币圈频道、财经播报或情绪鼓动。
不得改变方向、合约、NBBO、入场上限、失效位、目标位、有效期或最大亏损；不得补造数据。
TradeReady 只是未连接券商订单的行情候选告警，不得写成已挂单、已成交、已持仓或已撤单。
输出简短 Markdown，固定使用 Desk View、Execution、Risk、Targets、Timing 五部分。
只给一个主方向；相反方向只能作为当前交易的失效条件。禁用『需要看盘、半路、不追、剧本、砸、抢、扛、顶上』等口语。
决断体现在价格纪律和失效纪律，不得用夸张措辞制造确定性。"""


def process_trade_intent(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    settings: NotificationSettings | None = None,
    feature_policy: MarketFeatureSettings | None = None,
    order_policy: OrderMapPolicy | None = None,
    expected_policy_version: str | None = None,
    action_now: datetime | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Record every material gate result and deliver each ready event at most once."""

    now = _utc(now)
    state_path = _state_path(storage)
    latest_path = _latest_path(storage)
    signature = _signature(intent)
    intent_id = str(intent.get("intent_id") or "")
    ready = intent.get("status") == "trade_ready"
    expiry_reason = (
        _ready_contract_reason(
            intent,
            now=now,
            expected_policy_version=expected_policy_version,
        )
        if ready
        else None
    )
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        accepted = _accepted_events(state)
        semantic_keys = {
            str(key): str(value) for key, value in dict(state.get("semantic_keys") or {}).items()
        }
        semantic_key = str(intent.get("semantic_key") or "")
        semantic_scope = str(intent.get("semantic_scope") or "")
        if intent.get("phase") == "invalidated" and semantic_scope:
            invalidated_ids = {
                key
                for key, value in semantic_keys.items()
                if value == semantic_scope or value.startswith(f"{semantic_scope}|")
            }
            for key in invalidated_ids:
                accepted.pop(key, None)
                semantic_keys.pop(key, None)
        duplicate = bool(
            intent_id
            and (intent_id in accepted or (semantic_key and semantic_key in semantic_keys.values()))
        )
        inflight = {
            key: value
            for key, value in dict(state.get("inflight") or {}).items()
            if _lease_is_live(value, now=now)
        }
        delivery_in_progress = bool(intent_id and intent_id in inflight)
        if ready and not expiry_reason and intent_id and not duplicate and not delivery_in_progress:
            inflight[intent_id] = now.isoformat()
        atomic_write_json_secure(latest_path, dict(intent))
        if signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), dict(intent))
        state.update(
            {
                "schema_version": 2,
                "last_signature": signature,
                "last_status": intent.get("status"),
                "last_event_id": intent.get("event_id"),
                "updated_at": now.isoformat(),
                "accepted": accepted,
                "semantic_keys": semantic_keys,
                "inflight": inflight,
            }
        )
        state.pop("delivered", None)
        atomic_write_json_secure(state_path, state)

    if intent.get("status") != "trade_ready":
        return {
            "attempted": False,
            "delivered": False,
            "reason": str(intent.get("status") or "observing"),
        }
    if expiry_reason:
        return {"attempted": False, "delivered": False, "reason": expiry_reason}
    if not intent_id:
        return {"attempted": False, "delivered": False, "reason": "intent_id_unavailable"}
    if duplicate:
        return {"attempted": False, "delivered": False, "reason": "already_accepted"}
    if delivery_in_progress:
        return {"attempted": False, "delivered": False, "reason": "delivery_in_progress"}

    notification = settings or NotificationSettings.from_env()
    if not getattr(notification, "enabled", True):
        _release_delivery_lease(state_path, intent_id, now=now)
        return {"attempted": False, "delivered": False, "reason": "notification_disabled"}
    if not any(
        bool(getattr(notification, field, False))
        for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
    ):
        _release_delivery_lease(state_path, intent_id, now=now)
        return {"attempted": False, "delivered": False, "reason": "no_delivery_sink"}
    # The producer path is deterministic and local.  Re-read the wall clock
    # and latest-state projection immediately before the durable enqueue.
    action_now = _utc(action_now or _action_now())
    action_reason, action_evidence = _action_revalidation(
        storage,
        intent,
        now=action_now,
        feature_policy=feature_policy,
        order_policy=order_policy,
        expected_policy_version=expected_policy_version,
    )
    if action_reason:
        _record_action_revalidation(
            state_path,
            intent_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, intent_id, now=action_now)
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": action_reason,
            "action_revalidated_at": action_now.isoformat(),
        }
    event_occurred_at = _intent_occurred_at(intent)
    if event_occurred_at is None:
        action_evidence["reason"] = "intent_occurred_at_unavailable"
        _record_action_revalidation(
            state_path,
            intent_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, intent_id, now=action_now)
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": "intent_occurred_at_unavailable",
            "action_revalidated_at": action_now.isoformat(),
        }
    # The action-time quote is authoritative for the final gate and audit, but
    # the durable notification payload must remain the immutable decision
    # snapshot. Otherwise a crash between enqueue and state acknowledgement can
    # turn a harmless quote refresh into an event-id collision on replay.
    text = render_trade_intent(intent)
    enqueued = enqueue_notification(
        notification,
        NotificationEnvelope(
            event_id=intent_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=event_occurred_at,
        ),
        title="SPX TRADE READY",
        text=text,
        friend=True,
        feishu_text=text,
        enqueued_at=action_now,
    )
    if enqueued.accepted:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            accepted = _accepted_events(state)
            accepted[intent_id] = action_now.isoformat()
            if len(accepted) > 200:
                accepted = dict(sorted(accepted.items(), key=lambda item: item[1])[-200:])
            state["accepted"] = accepted
            state.pop("delivered", None)
            semantic_keys = {
                str(key): str(value)
                for key, value in dict(state.get("semantic_keys") or {}).items()
                if key in accepted
            }
            semantic_key = str(intent.get("semantic_key") or "")
            if semantic_key:
                semantic_keys[intent_id] = semantic_key
            state["semantic_keys"] = semantic_keys
            inflight = dict(state.get("inflight") or {})
            inflight.pop(intent_id, None)
            state["inflight"] = inflight
            state["last_action_revalidation"] = action_evidence
            state["updated_at"] = action_now.isoformat()
            atomic_write_json_secure(state_path, state)
    else:
        _record_action_revalidation(
            state_path,
            intent_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, intent_id, now=action_now)
    return {
        "attempted": True,
        "accepted": enqueued.accepted,
        "inserted": enqueued.inserted,
        "duplicate": enqueued.duplicate,
        "delivered": enqueued.delivered,
        "queued": enqueued.queued_for_recovery,
        "outcome": enqueued.outcome,
        "writer": "template",
        "targets": list(enqueued.targets),
        "action_revalidated_at": action_now.isoformat(),
    }


def render_trade_intent(intent: Mapping[str, object]) -> str:
    direction = "向上突破买 Call" if intent.get("direction") == "up" else "向下突破买 Put"
    if intent.get("thesis") == "fade":
        direction = "拒绝下破买 Call" if intent.get("direction") == "up" else "拒绝上破买 Put"
    lines = [
        f"TRADE READY｜{direction}",
        "## 主剧本",
        f"SPX **{_fmt(intent.get('spx_spot'))}**，关键位 **{_fmt(intent.get('trigger_level'))}**，"
        f"确认后延伸 **{_fmt(intent.get('follow_through_points'))} 点**。",
        "## 执行",
        f"**{intent.get('contract_label')}**　决策快照 `"
        f"{_fmt(intent.get('decision_bid'))} / {_fmt(intent.get('decision_ask'))}`　"
        f"买入上限 `{_fmt(intent.get('entry_limit'))}`",
        f"数据源 {intent.get('provider')}　源时间 {intent.get('quote_source_at')}",
        "## 风险",
        f"SPX 回到 **{_fmt(intent.get('invalidation_spx'))}** 失效　"
        f"目标 **{_fmt(intent.get('target_spx'))}**　"
        f"剩余空间 **{_fmt(intent.get('remaining_target_room_points'))} 点**　"
        f"收益风险比 **{_fmt(intent.get('remaining_reward_risk'))}**　"
        f"单张最大权利金 `${_fmt(intent.get('max_loss_per_contract'))}`",
    ]
    lines.extend(_greek_risk_lines(intent.get("greek_confidence")))
    lines.extend(_moving_average_lines(intent.get("moving_average_context")))
    lines.extend(_pilot_diagnostic_lines(intent.get("pilot_diagnostics")))
    lines.extend(_play_stats_lines(intent.get("play_stats")))
    lines.extend(
        [
            "## 时效",
            f"意图过期 `{intent.get('valid_until') or intent.get('expires_at')}`　"
            f"时间止损 `{intent.get('time_stop_at')}`",
            "未连接真实订单、成交或持仓状态；这是行情条件候选，数量由人工确认。",
        ]
    )
    return "\n".join(lines)


def _greek_risk_lines(score: object) -> list[str]:
    if not isinstance(score, Mapping):
        return []
    delta = score.get("delta")
    gamma = score.get("gamma_per_point")
    theta_loss = score.get("theta_15m_loss_fraction")
    crush_loss = score.get("iv_down_3vol_loss_fraction")
    if not any(isinstance(value, int | float) for value in (delta, gamma, theta_loss, crush_loss)):
        return []
    return [
        "## 希腊风险",
        f"Delta `{_fmt(delta)}`　Gamma `{_fmt(gamma)}`　"
        f"15分钟Theta情景 `{_fmt_pct(theta_loss)}`　"
        f"IV下降3波动点情景 `{_fmt_pct(crush_loss)}`　"
        f"评分 `{_fmt(score.get('raw_score'))}`",
        "希腊字母只用于合约与持有风险，不改变墙位方向。",
    ]


def _pilot_diagnostic_lines(value: object) -> list[str]:
    if not isinstance(value, list | tuple) or not value:
        return []
    labels = "、".join(str(item) for item in value if item)
    return ["## 辅助确认", f"非否决诊断：{labels}"] if labels else []


def _moving_average_lines(value: object) -> list[str]:
    if not isinstance(value, Mapping):
        return []
    price = value.get("price")
    sma20 = value.get("sma20")
    sma50 = value.get("sma50")
    sma200 = value.get("sma200")
    if not any(
        isinstance(item, int | float)
        for item in (price, sma20, sma50, sma200)
    ):
        return []
    spx20 = value.get("spx_equivalent_sma20")
    spx50 = value.get("spx_equivalent_sma50")
    spx200 = value.get("spx_equivalent_sma200")
    projection = (
        "　SPX基差投影 "
        f"`{_fmt_fixed(spx20)} / {_fmt_fixed(spx50)} / {_fmt_fixed(spx200)}`"
        if any(
            isinstance(item, int | float)
            for item in (spx20, spx50, spx200)
        )
        else ""
    )
    precision = (
        "　`贴线区：不确认突破`"
        if value.get("spx_projection_near_line") is True
        else ""
    )
    regime = str(value.get("regime_state") or "-")
    regime_direction = str(value.get("regime_direction") or "-")
    convexity = str(value.get("same_direction_convexity") or "-")
    cross = str(value.get("cross_direction") or "-")
    cross_age = value.get("bars_since_cross")
    cross_age_text = str(int(cross_age)) if isinstance(cross_age, int | float) else "-"
    persistent = _bool_text(value.get("cross_persistent_2_bars"))
    fresh = _bool_text(value.get("cross_fresh"))
    regime_guard = _moving_average_regime_guard(regime)
    return [
        "## 均线位置",
        f"ES 5m P/MA20/MA50/MA200 "
        f"`{_fmt_fixed(price)} / {_fmt_fixed(sma20)} / {_fmt_fixed(sma50)} / "
        f"{_fmt_fixed(sma200)}`　"
        f"状态 `{value.get('relation') or '-'}`{projection}{precision}",
        "MA50/200 "
        f"`{regime}`　方向 `{regime_direction}`　同向凸性 `{convexity}`　"
        f"距MA50/200 ATR `{_fmt_fixed(value.get('distance_to_sma50_atr'))} / "
        f"{_fmt_fixed(value.get('distance_to_sma200_atr'))}`　"
        f"斜率3/6根 MA50 `{_fmt_fixed(value.get('ma50_slope_3_atr'))} / "
        f"{_fmt_fixed(value.get('ma50_slope_6_atr'))}`、"
        f"MA200 `{_fmt_fixed(value.get('ma200_slope_3_atr'))} / "
        f"{_fmt_fixed(value.get('ma200_slope_6_atr'))}`　"
        f"间距 `{_fmt_fixed(value.get('ma50_ma200_spread_atr'))} ATR`"
        f"（3根Δ `{_fmt_fixed(value.get('spread_change_3_atr'))}`）　"
        f"交叉 `{cross}` 已 {cross_age_text} 根，持续2根 `{persistent}`，"
        f"新鲜 `{fresh}`。",
        f"{regime_guard} 均线与交叉不单独生成方向、入场或自动动作。",
        "SPX数值使用同步 ES−SPX 基差投影，不是 SPX 自身历史均线；本项只读。",
    ]


def _moving_average_regime_guard(regime: str) -> str:
    if regime == "TREND_EXTENDED":
        return "趋势已延伸：禁止追同向凸性；"
    if regime in {"REGIME_TRANSITION", "MIXED"}:
        return "结构未统一：必须等待 wall/flip 接受或拒绝确认；"
    return "仅作 wall/flip 条件的趋势背景；"


def _bool_text(value: object) -> str:
    return "是" if value is True else "否" if value is False else "-"


def _play_stats_lines(stats: object) -> list[str]:
    if not isinstance(stats, Mapping):
        return []
    play = str(stats.get("play") or "")
    level_kind = str(stats.get("level_kind") or "")
    sample_count = stats.get("sample_count")
    winrate = stats.get("winrate")
    avg_return = stats.get("avg_return_fraction")
    if (
        not play
        or not level_kind
        or not isinstance(sample_count, int | float)
        or not isinstance(winrate, int | float)
        or not isinstance(avg_return, int | float)
    ):
        return []
    return [
        "## 同类信号",
        f"近{stats.get('window_days')}日 {play}@{level_kind}（{stats.get('horizon_seconds')}s口径）: "
        f"n={int(sample_count)} 胜率{float(winrate) * 100:.0f}% 均值{float(avg_return) * 100:+.1f}%",
    ]


def _fmt_pct(value: object) -> str:
    return f"{float(value) * 100:.2f}%" if isinstance(value, int | float) else "-"


def _writer_prompt(intent: Mapping[str, object], template: str) -> str:
    return (
        "把下面已经通过确定性门控的交易意图排成易扫读飞书消息。只做解释和排版，不重新判断。\n"
        "MA50/MA200及交叉只作只读背景：不得仅凭金叉/死叉生成Call/Put、改变墙位方向或"
        "覆盖wall/flip确认；TREND_EXTENDED必须写明禁止追同向凸性，"
        "REGIME_TRANSITION/MIXED必须写明等待wall/flip接受或拒绝确认。\n"
        f"事实 JSON:\n{json.dumps(dict(intent), ensure_ascii=False, sort_keys=True)}\n"
        f"确定性模板:\n{template}"
    )


def _writer_output_valid(text: str, intent: Mapping[str, object]) -> bool:
    required = [
        str(intent.get("contract_label") or ""),
        _fmt(intent.get("entry_limit")),
        _fmt(intent.get("invalidation_spx")),
        _fmt(intent.get("target_spx")),
    ]
    stats = intent.get("play_stats")
    if isinstance(stats, Mapping):
        play = str(stats.get("play") or "")
        level_kind = str(stats.get("level_kind") or "")
        sample_count = stats.get("sample_count")
        winrate = stats.get("winrate")
        if (
            play
            and level_kind
            and isinstance(sample_count, int | float)
            and isinstance(winrate, int | float)
        ):
            required.extend(
                (
                    play,
                    level_kind,
                    f"n={int(sample_count)}",
                    f"{float(winrate) * 100:.0f}%",
                )
            )
    moving = intent.get("moving_average_context")
    if isinstance(moving, Mapping):
        regime = str(moving.get("regime_state") or "")
        if regime:
            required.extend(("均线", regime))
        if regime == "TREND_EXTENDED":
            required.append("禁止追同向凸性")
        elif regime in {"REGIME_TRANSITION", "MIXED"}:
            required.append("wall/flip")
    forbidden_ma_triggers = (
        "金叉买Call",
        "金叉买 Call",
        "死叉买Put",
        "死叉买 Put",
        "交叉即买",
        "仅凭金叉",
        "仅凭死叉",
    )
    if any(token in text for token in forbidden_ma_triggers):
        return False
    return bool(text.strip()) and all(token and token in text for token in required)


def _signature(intent: Mapping[str, object]) -> str:
    material = {
        key: intent.get(key)
        for key in (
            "status",
            "event_id",
            "play",
            "contract_id",
            "entry_limit",
            "invalidation_spx",
            "target_spx",
            "block_reasons",
            "schema_version",
            "policy_version",
            "valid_until",
            "coordinate",
        )
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]


def _ready_contract_reason(
    intent: Mapping[str, object],
    *,
    now: datetime,
    expected_policy_version: str | None = None,
) -> str | None:
    """Enforce v3; only explicitly versioned v1 events receive one-cycle fallback."""

    schema_version = intent.get("schema_version")
    if schema_version == STRATEGY_EVENT_SCHEMA_VERSION:
        issues = actionable_strategy_contract_issues(intent, now=now)
        if issues:
            if "strategy_event_expired" in issues:
                return "intent_expired"
            return issues[0]
        source_policy = str(intent.get("policy_version") or "")
        if not source_policy.startswith("rth_trade_intent.v3+sha256:"):
            return "source_policy_incompatible"
        if expected_policy_version and source_policy != expected_policy_version:
            return "source_policy_version_drift"
        coordinate = intent.get("coordinate")
        if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "official_spx":
            return "source_coordinate_mismatch"
        return None
    if schema_version == 1:
        expires_at = _datetime(intent.get("expires_at"))
        if expires_at is None:
            return "intent_expiry_unavailable"
        return "intent_expired" if now >= expires_at else None
    return "strategy_schema_unsupported"


def _action_revalidation(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    feature_policy: MarketFeatureSettings | None,
    expected_policy_version: str | None,
    order_policy: OrderMapPolicy | None = None,
) -> tuple[str | None, dict[str, object]]:
    """Fail closed at enqueue time and, in production, reload the market projection."""

    now = _utc(now)
    evidence: dict[str, object] = {
        "intent_id": intent.get("intent_id"),
        "decision_evaluated_at": intent.get("evaluated_at"),
        "action_revalidated_at": now.isoformat(),
        "expected_policy_version": expected_policy_version,
        "source_policy_version": intent.get("policy_version"),
    }
    reason = _ready_contract_reason(
        intent,
        now=now,
        expected_policy_version=expected_policy_version,
    )
    if reason:
        evidence["reason"] = reason
        return reason, evidence
    if feature_policy is None:
        reason = "action_feature_policy_unavailable"
        evidence["quote_revalidation"] = "blocked"
        evidence["reason"] = reason
        return reason, evidence

    latest = LatestStateStore(storage).load(now=now)
    evidence["quote_revalidation"] = "performed"
    evidence["quote_state_created_at"] = latest.created_at.isoformat()
    contract_id = str(intent.get("contract_id") or "")
    quote = latest.best_quote(contract_id) if contract_id else None
    if quote is None:
        reason = "action_quote_unavailable"
        evidence["reason"] = reason
        return reason, evidence
    source_at = quote.quote_time or quote.trade_time
    transport_at = quote.last_update_at or quote.received_at
    bid = float(quote.bid) if isinstance(quote.bid, int | float) else None
    mid = float(quote.mid) if isinstance(quote.mid, int | float) else None
    ask = float(quote.ask) if isinstance(quote.ask, int | float) else None
    entry_limit = _number(intent.get("entry_limit"))
    entry_fraction = _number(intent.get("entry_spread_fraction"))
    evidence.update(
        {
            "contract_id": contract_id or None,
            "provider": quote.provider.value,
            "quote_source_at": source_at.isoformat() if source_at is not None else None,
            "quote_transport_at": transport_at.isoformat(),
            "bid": bid,
            "mid": mid,
            "ask": ask,
            "entry_limit": entry_limit,
        }
    )
    intent_provider = str(intent.get("provider") or "")
    if not intent_provider:
        reason = "action_quote_provider_unavailable"
    elif intent_provider != quote.provider.value:
        reason = "action_quote_provider_mismatch"
    elif bid is None or mid is None or ask is None or not 0 <= bid <= mid <= ask:
        reason = "action_quote_nbbo_invalid"
    elif source_at is None:
        reason = "action_quote_source_time_unavailable"
    elif entry_limit is None or entry_limit <= 0:
        reason = "action_entry_limit_invalid"
    elif entry_fraction is None or not 0.0 <= entry_fraction <= 1.0:
        reason = "action_entry_rule_invalid"
    else:
        source_age = (now - _utc(source_at)).total_seconds()
        transport_age = (now - _utc(transport_at)).total_seconds()
        evidence["source_age_seconds"] = source_age
        evidence["transport_age_seconds"] = transport_age
        tolerance = max(0.0, feature_policy.provider_sync_tolerance_seconds)
        if source_age < -tolerance:
            reason = "action_quote_source_in_future"
        elif source_age > feature_policy.trade_quote_max_age_seconds:
            reason = "action_quote_source_stale"
        elif transport_age < -tolerance:
            reason = "action_quote_transport_in_future"
        elif transport_age > feature_policy.trade_quote_max_age_seconds:
            reason = "action_quote_transport_stale"
        else:
            use = configured_quote_use_decision(quote, as_of=now)
            evidence["quote_quality_reason"] = use.reason
            if not use.pricing_allowed:
                reason = "action_quote_not_pricing_allowed"
            else:
                execution_gate = evaluate_execution_quote(
                    quote,
                    latest.quotes,
                    as_of=now,
                    policy=order_policy or DEFAULT_ORDER_MAP_POLICY,
                )
                evidence["execution_quote_gate"] = execution_gate.to_dict()
                if not execution_gate.executable:
                    reason = f"action_execution_quote_{execution_gate.reasons[0]}"
                else:
                    action_limit = round_to_tick(min(mid, bid + entry_fraction * (ask - bid)))
                    evidence["recomputed_entry_limit"] = action_limit
                    reason = (
                        None
                        if math.isclose(action_limit, entry_limit, abs_tol=1e-9)
                        else "action_entry_limit_changed"
                    )
    evidence["reason"] = reason
    return reason, evidence


def _action_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _record_action_revalidation(
    state_path: Path,
    intent_id: str,
    *,
    now: datetime,
    evidence: Mapping[str, object],
) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        state["last_action_revalidation"] = dict(evidence)
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)


def _intent_occurred_at(intent: Mapping[str, object]) -> datetime | None:
    """Return an immutable timestamp for idempotent outbox replays."""

    coordinate = intent.get("coordinate")
    coordinate_as_of = coordinate.get("as_of") if isinstance(coordinate, Mapping) else None
    for value in (
        intent.get("evaluated_at"),
        coordinate_as_of,
        intent.get("quote_source_at"),
        intent.get("valid_until"),
        intent.get("expires_at"),
    ):
        parsed = _datetime(value)
        if parsed is not None:
            return parsed
    return None


def _accepted_events(state: Mapping[str, object]) -> dict[str, str]:
    """Migrate the v1 ``delivered`` projection to v2 durable acceptance."""

    legacy = dict(state.get("delivered") or {})
    current = dict(state.get("accepted") or {})
    return {
        str(key): str(value)
        for key, value in {**legacy, **current}.items()
        if str(key) and str(value)
    }


def _fmt(value: object) -> str:
    if not isinstance(value, int | float):
        return "-"
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _fmt_fixed(value: object) -> str:
    return f"{float(value):.2f}" if isinstance(value, int | float) else "-"


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "trade_intent_delivery_state.json"


def _latest_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "trade_intent.json"


def _audit_path(storage: StorageSettings, now: datetime) -> Path:
    return (
        Path(storage.data_root)
        / "features"
        / "trade_intents"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _lease_is_live(value: object, *, now: datetime) -> bool:
    started_at = _datetime(value)
    return bool(
        started_at is not None
        and 0.0 <= (now - started_at).total_seconds() < DELIVERY_LEASE_SECONDS
    )


def _release_delivery_lease(state_path: Path, intent_id: str, *, now: datetime) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        inflight = dict(state.get("inflight") or {})
        inflight.pop(intent_id, None)
        state["inflight"] = inflight
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)
