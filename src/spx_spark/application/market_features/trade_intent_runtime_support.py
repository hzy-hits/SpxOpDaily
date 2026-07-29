"""Deterministic presentation and persistence helpers for TradeIntent runtime."""

from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.notifier.operator_cards import (
    beijing_time,
    decision_now,
    option_contract_label,
    remaining_seconds,
)


DELIVERY_LEASE_SECONDS = 120.0
DELIVERY_LEASE_TTL_FRACTION = 0.25


TRADE_INTENT_SYSTEM_PROMPT = """你是 SPX 指数期权自营台的 execution trader，只负责排版一条已经通过代码硬门槛的 0DTE 交易意图。
写成机构级 execution ticket，不是散户喊单、币圈频道、财经播报或情绪鼓动。
不得改变方向、合约、NBBO、入场上限、失效位、目标位、有效期或最大亏损；不得补造数据。
TradeReady 只是未连接券商订单的行情候选告警，不得写成已挂单、已成交、已持仓或已撤单。
输出简短 Markdown，固定使用 Desk View、Execution、Risk、Targets、Timing 五部分。
只给一个主方向；相反方向只能作为当前交易的失效条件。禁用『需要看盘、半路、不追、剧本、砸、抢、扛、顶上』等口语。
决断体现在价格纪律和失效纪律，不得用夸张措辞制造确定性。"""


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
    return f"SPX {level} {condition}；现价 {_fmt_fixed(intent.get('spx_spot'))}"


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


def _delivery_lease_seconds(
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> float:
    valid_until = _datetime(intent.get("valid_until") or intent.get("expires_at"))
    if valid_until is None:
        return DELIVERY_LEASE_SECONDS
    remaining = max(0.0, (valid_until - _utc(now)).total_seconds())
    if remaining <= 0.0:
        return 0.0
    return min(
        DELIVERY_LEASE_SECONDS,
        remaining,
        max(1.0, remaining * DELIVERY_LEASE_TTL_FRACTION),
    )


def _delivery_lease(
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    now = _utc(now)
    seconds = _delivery_lease_seconds(intent, now=now)
    return {
        "started_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=seconds)).isoformat(),
        "lease_seconds": seconds,
    }


def _lease_is_live(
    value: object,
    *,
    now: datetime,
    max_seconds: float = DELIVERY_LEASE_SECONDS,
) -> bool:
    if isinstance(value, Mapping):
        started_at = _datetime(value.get("started_at"))
        expires_at = _datetime(value.get("expires_at"))
    else:
        # Backward-compatible cap for v3 state written before leases carried an
        # explicit expiry.
        started_at = _datetime(value)
        expires_at = None
    if started_at is None:
        return False
    now = _utc(now)
    age = (now - started_at).total_seconds()
    legacy_max_seconds = max(0.0, max_seconds)
    if (
        age < 0.0
        or age >= DELIVERY_LEASE_SECONDS
        or (expires_at is None and age >= legacy_max_seconds)
    ):
        return False
    if expires_at is not None and now >= expires_at:
        return False
    return True
