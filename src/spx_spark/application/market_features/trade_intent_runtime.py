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
from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.dispatcher import (
    cancel_pending_notification,
    enqueue_notification,
    notification_event_exists,
)
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.operator_cards import (
    beijing_time,
    decision_now,
    option_contract_label,
    option_contract_right,
    parse_time,
    remaining_seconds,
)
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
    delivery_event_id = _trade_ready_delivery_event_id(intent) if ready else ""
    notification = settings or NotificationSettings.from_env()
    durable_event_exists = bool(
        ready
        and delivery_event_id
        and notification_event_exists(notification, delivery_event_id)
    )
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
        semantic_dedupe_key = semantic_key or (
            f"intent:{intent_id}" if intent_id else ""
        )
        semantic_scope = str(intent.get("semantic_scope") or "")
        lifecycle_events = {
            str(item.get("event_id") or ""): {
                "semantic_key": str(item.get("semantic_key") or ""),
                "semantic_scope": str(item.get("semantic_scope") or ""),
            }
            for item in state.get("delivery_lifecycle_events") or []
            if isinstance(item, Mapping) and item.get("event_id")
        }
        cancellation_pending = {
            str(item)
            for item in state.get(
                "pending_delivery_cancellation_event_ids"
            )
            or []
            if item
        }
        if ready and delivery_event_id:
            lifecycle_events.setdefault(
                delivery_event_id,
                {
                    "semantic_key": semantic_dedupe_key,
                    "semantic_scope": semantic_scope,
                },
            )
        inflight = {
            key: value
            for key, value in dict(state.get("inflight") or {}).items()
            if _lease_is_live(value, now=now)
        }
        if intent.get("phase") == "invalidated":
            invalidated_ids = {
                key
                for key, value in semantic_keys.items()
                if not semantic_scope
                or value == semantic_scope
                or value.startswith(f"{semantic_scope}|")
            }
            invalidated_ids.update(
                event_id
                for event_id, lifecycle in lifecycle_events.items()
                if not semantic_scope
                or lifecycle["semantic_scope"] == semantic_scope
                or lifecycle["semantic_key"] == semantic_scope
                or lifecycle["semantic_key"].startswith(
                    f"{semantic_scope}|"
                )
            )
            # Migrate a v2 producer that crashed after enqueue but before it
            # could persist the v3 lifecycle registry.
            invalidated_ids.update(inflight)
            cancellation_pending.update(invalidated_ids)
        for key in sorted(cancellation_pending):
            try:
                cancel_pending_notification(
                    notification,
                    key,
                    now=now,
                    reason="trade_intent_lifecycle_invalidated",
                )
            except Exception:
                # The lifecycle record makes this cancellation replayable even
                # if the outbox enqueue succeeded before producer state-ack.
                continue
            cancellation_pending.discard(key)
            accepted.pop(key, None)
            semantic_keys.pop(key, None)
            lifecycle_events.pop(key, None)
            inflight.pop(key, None)
        if durable_event_exists and delivery_event_id:
            accepted.setdefault(delivery_event_id, now.isoformat())
            if semantic_dedupe_key:
                semantic_keys[delivery_event_id] = semantic_dedupe_key
        delivery_blocked_by_cancellation = bool(
            ready and cancellation_pending
        )
        duplicate = bool(
            delivery_event_id
            and (
                delivery_event_id in accepted
                or (
                    semantic_dedupe_key
                    and semantic_dedupe_key in semantic_keys.values()
                )
            )
        )
        if durable_event_exists:
            inflight.pop(delivery_event_id, None)
        delivery_in_progress = bool(
            delivery_event_id and delivery_event_id in inflight
        )
        if (
            ready
            and not expiry_reason
            and delivery_event_id
            and not duplicate
            and not delivery_blocked_by_cancellation
            and not delivery_in_progress
        ):
            inflight[delivery_event_id] = now.isoformat()
        atomic_write_json_secure(latest_path, dict(intent))
        if signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), dict(intent))
        state.update(
            {
                "schema_version": 3,
                "last_signature": signature,
                "last_status": intent.get("status"),
                "last_event_id": intent.get("event_id"),
                "last_delivery_event_id": delivery_event_id or None,
                "updated_at": now.isoformat(),
                "accepted": accepted,
                "semantic_keys": semantic_keys,
                "inflight": inflight,
                "delivery_lifecycle_events": [
                    {
                        "event_id": event_id,
                        **lifecycle,
                    }
                    for event_id, lifecycle in sorted(
                        lifecycle_events.items()
                    )[-200:]
                ],
                "pending_delivery_cancellation_event_ids": sorted(
                    cancellation_pending
                )[-200:],
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
    if not delivery_event_id:
        return {
            "attempted": False,
            "delivered": False,
            "reason": "notification_event_id_unavailable",
        }
    if delivery_blocked_by_cancellation:
        return {
            "attempted": False,
            "delivered": False,
            "reason": "lifecycle_cancellation_pending",
        }
    if duplicate:
        if durable_event_exists:
            return {
                "attempted": False,
                "delivered": False,
                "accepted": True,
                "inserted": False,
                "duplicate": True,
                "reason": "outbox_event_reconciled",
            }
        return {"attempted": False, "delivered": False, "reason": "already_accepted"}
    if delivery_in_progress:
        return {"attempted": False, "delivered": False, "reason": "delivery_in_progress"}

    if not getattr(notification, "enabled", True):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
        return {"attempted": False, "delivered": False, "reason": "notification_disabled"}
    if not any(
        bool(getattr(notification, field, False))
        for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
    ):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
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
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": action_reason,
            "action_revalidated_at": action_now.isoformat(),
        }
    card_reason = _manual_card_contract_reason(intent, now=action_now)
    if card_reason:
        action_evidence["reason"] = card_reason
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": card_reason,
            "action_revalidated_at": action_now.isoformat(),
        }
    event_occurred_at = _intent_occurred_at(intent)
    if event_occurred_at is None:
        action_evidence["reason"] = "intent_occurred_at_unavailable"
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
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
            event_id=delivery_event_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=event_occurred_at,
            expires_at=parse_time(intent.get("valid_until") or intent.get("expires_at")),
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
            accepted[delivery_event_id] = action_now.isoformat()
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
            semantic_dedupe_key = semantic_key or (
                f"intent:{intent_id}" if intent_id else ""
            )
            if semantic_dedupe_key:
                semantic_keys[delivery_event_id] = semantic_dedupe_key
            state["semantic_keys"] = semantic_keys
            inflight = dict(state.get("inflight") or {})
            inflight.pop(delivery_event_id, None)
            state["inflight"] = inflight
            state["last_action_revalidation"] = action_evidence
            state["updated_at"] = action_now.isoformat()
            atomic_write_json_secure(state_path, state)
    else:
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
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
    """Render the complete manual ticket before any analytical context."""

    direction = str(intent.get("direction") or "")
    right = "CALL" if direction == "up" else "PUT"
    valid_until = intent.get("valid_until") or intent.get("expires_at")
    # Keep the durable payload byte-identical across outbox crash replays.
    # The action boundary independently revalidates current quote/expiry.
    render_now = decision_now(intent)
    ttl = remaining_seconds(valid_until, now=render_now)
    ttl_text = f"剩余 {ttl} 秒" if ttl is not None else "时效未知"
    contract = option_contract_label(
        intent.get("contract_id"),
        fallback=intent.get("contract_label"),
    )
    return "\n".join(
        (
            f"🟢 MANUAL READY · {right}",
            "类型  单腿 · 仅人工提交",
            f"买入  {contract}",
            f"NBBO  {_fmt_fixed(intent.get('decision_bid'))} / "
            f"{_fmt_fixed(intent.get('decision_ask'))}（决策快照）",
            f"限价  ≤ {_fmt_fixed(intent.get('entry_limit'))}",
            f"触发  {_operator_trigger(intent)}",
            f"止损  {_operator_invalidation(intent)}",
            f"目标  SPX {_fmt_fixed(intent.get('target_spx'))}",
            f"退出  {beijing_time(intent.get('time_stop_at'))}",
            f"有效  决策时{ttl_text}（至 {beijing_time(valid_until, seconds=True)}）；"
            "提交前重新报价",
            f"风险  单张最大权利金 ${_fmt_fixed(intent.get('max_loss_per_contract'))}；"
            "数量由人工确认",
            f"解释  {_operator_explanation(intent)}",
            "权限  自动下单关闭；未连接真实订单、成交或持仓状态",
        )
    )


def _operator_trigger(intent: Mapping[str, object]) -> str:
    level = _fmt_fixed(intent.get("trigger_level"))
    direction = str(intent.get("direction") or "")
    thesis = str(intent.get("thesis") or "")
    if thesis == "fade":
        condition = "拒绝下破并确认" if direction == "up" else "拒绝上破并确认"
    else:
        condition = "突破并保持" if direction == "up" else "跌破并保持"
    return (
        f"SPX {level} {condition}；现价 {_fmt_fixed(intent.get('spx_spot'))}"
    )


def _operator_invalidation(intent: Mapping[str, object]) -> str:
    relation = "跌回" if intent.get("direction") == "up" else "收回"
    return f"SPX {relation} {_fmt_fixed(intent.get('invalidation_spx'))}"


def _operator_explanation(intent: Mapping[str, object]) -> str:
    direction = str(intent.get("direction") or "")
    thesis = str(intent.get("thesis") or "")
    if thesis == "fade":
        return (
            "关键位拒绝下破已确认，执行反弹 Call"
            if direction == "up"
            else "关键位拒绝上破已确认，执行回落 Put"
        )
    return (
        "关键位向上接受已确认，执行突破 Call"
        if direction == "up"
        else "关键位向下接受已确认，执行跌破 Put"
    )


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
        option_contract_label(
            intent.get("contract_id"),
            fallback=intent.get("contract_label"),
        ),
        _fmt_fixed(intent.get("decision_bid")),
        _fmt_fixed(intent.get("decision_ask")),
        _fmt(intent.get("entry_limit")),
        _fmt(intent.get("invalidation_spx")),
        _fmt(intent.get("target_spx")),
        "MANUAL READY",
        "提交前重新报价",
        "自动下单关闭",
    ]
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


def _trade_ready_delivery_event_id(intent: Mapping[str, object]) -> str:
    """Separate stable strategy identity from one confirmed lifecycle delivery."""

    intent_id = str(intent.get("intent_id") or "")
    lifecycle_event_id = str(intent.get("event_id") or "")
    if not intent_id or not lifecycle_event_id:
        return ""
    digest = hashlib.sha256(
        f"trade_ready_notification.v2|{intent_id}|{lifecycle_event_id}".encode()
    ).hexdigest()[:20]
    return f"{intent_id}:notify:{digest}"


def _ready_contract_reason(
    intent: Mapping[str, object],
    *,
    now: datetime,
    expected_policy_version: str | None = None,
) -> str | None:
    """Enforce the current v3 contract and explicit manual-alert authority."""

    schema_version = intent.get("schema_version")
    if schema_version == STRATEGY_EVENT_SCHEMA_VERSION:
        authority_issues = live_trade_intent_authority_issues(intent)
        if authority_issues:
            return authority_issues[0]
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
    reason = _manual_card_contract_reason(intent, now=now)
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
    source_at = quote.quote_time
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


def _manual_card_contract_reason(
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> str | None:
    """Reject a green card unless every operator field is complete and coherent."""

    direction = str(intent.get("direction") or "")
    if direction not in {"up", "down"}:
        return "manual_card_direction_invalid"
    thesis = str(intent.get("thesis") or "")
    if thesis not in {"breakout", "fade"}:
        return "manual_card_thesis_invalid"
    right = option_contract_right(intent.get("contract_id"))
    expected_right = "C" if direction == "up" else "P"
    if right is None:
        return "manual_card_exact_contract_unavailable"
    if right != expected_right:
        return "manual_card_contract_direction_mismatch"

    numeric_fields = (
        "decision_bid",
        "decision_ask",
        "entry_limit",
        "trigger_level",
        "spx_spot",
        "invalidation_spx",
        "target_spx",
        "max_loss_per_contract",
    )
    values: dict[str, float] = {}
    for field in numeric_fields:
        value = _number(intent.get(field))
        if value is None:
            return f"manual_card_field_missing:{field}"
        values[field] = value
    bid = values["decision_bid"]
    ask = values["decision_ask"]
    entry = values["entry_limit"]
    if bid < 0 or ask <= 0 or bid > ask:
        return "manual_card_nbbo_invalid"
    if entry <= 0 or not bid <= entry <= ask:
        return "manual_card_entry_limit_outside_nbbo"
    if any(
        values[field] <= 0
        for field in ("trigger_level", "spx_spot", "invalidation_spx", "target_spx")
    ):
        return "manual_card_spx_coordinate_invalid"
    if values["max_loss_per_contract"] <= 0 or not math.isclose(
        values["max_loss_per_contract"],
        entry * 100.0,
        abs_tol=0.01,
    ):
        return "manual_card_max_loss_inconsistent"

    trigger = values["trigger_level"]
    spot = values["spx_spot"]
    invalidation = values["invalidation_spx"]
    target = values["target_spx"]
    if direction == "up" and not invalidation < trigger < target:
        return "manual_card_risk_coordinates_incoherent"
    if direction == "down" and not target < trigger < invalidation:
        return "manual_card_risk_coordinates_incoherent"
    if direction == "up" and not invalidation < spot < target:
        return "manual_card_spot_outside_risk_bounds"
    if direction == "down" and not target < spot < invalidation:
        return "manual_card_spot_outside_risk_bounds"

    if not str(intent.get("provider") or ""):
        return "action_quote_provider_unavailable"
    if parse_time(intent.get("quote_source_at")) is None:
        return "action_quote_source_time_unavailable"
    valid_until = parse_time(intent.get("valid_until") or intent.get("expires_at"))
    if valid_until is None:
        return "manual_card_expiry_unavailable"
    if valid_until <= _utc(now):
        return "manual_card_expired"
    time_stop = parse_time(intent.get("time_stop_at"))
    if time_stop is None:
        return "manual_card_time_stop_unavailable"
    if time_stop <= _utc(now):
        return "manual_card_time_stop_elapsed"
    return None


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
