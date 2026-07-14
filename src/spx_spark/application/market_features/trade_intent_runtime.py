"""Persistence and human delivery for deterministic trade-ready intents."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.llm_writer import generate_push_text, record_push
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


DELIVERY_LEASE_SECONDS = 120.0


TRADE_INTENT_SYSTEM_PROMPT = """你只负责排版一条已经通过代码硬门槛的 SPX 交易意图。
不得改变方向、合约、入场上限、失效位、目标位、有效期或最大亏损；不得补造数据。
输出简短 Markdown，必须保留主剧本、执行、失效、目标、时效五部分。不要给出第二套相反方向方案。"""


def process_trade_intent(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    settings: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Record every material gate result and deliver each ready event at most once."""

    now = _utc(now)
    state_path = _state_path(storage)
    latest_path = _latest_path(storage)
    signature = _signature(intent)
    intent_id = str(intent.get("intent_id") or "")
    ready = intent.get("status") == "trade_ready"
    expires_at = _datetime(intent.get("expires_at"))
    expiry_reason = (
        "intent_expiry_unavailable"
        if ready and expires_at is None
        else "intent_expired"
        if ready and expires_at is not None and now >= expires_at
        else None
    )
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        delivered = dict(state.get("delivered") or {})
        semantic_keys = {
            str(key): str(value)
            for key, value in dict(state.get("semantic_keys") or {}).items()
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
                delivered.pop(key, None)
                semantic_keys.pop(key, None)
        duplicate = bool(
            intent_id
            and (
                intent_id in delivered
                or (semantic_key and semantic_key in semantic_keys.values())
            )
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
                "schema_version": 1,
                "last_signature": signature,
                "last_status": intent.get("status"),
                "last_event_id": intent.get("event_id"),
                "updated_at": now.isoformat(),
                "delivered": delivered,
                "semantic_keys": semantic_keys,
                "inflight": inflight,
            }
        )
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
        return {"attempted": False, "delivered": False, "reason": "already_delivered"}
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
    template = render_trade_intent(intent)
    text, writer = generate_push_text(
        template,
        _writer_prompt(intent, template),
        notification,
        runner=runner,
        system=TRADE_INTENT_SYSTEM_PROMPT,
    )
    if writer != "template" and not _writer_output_valid(text, intent):
        text, writer = template, "template_validation_fallback"
    dispatch = dispatch_notification(
        notification,
        NotificationEnvelope(
            event_id=intent_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=now,
        ),
        title="SPX TRADE READY",
        text=text,
        friend=True,
        feishu_text=text,
        runner=runner,
        attempted_at=now,
    )
    sinks = list(dispatch.sinks)
    delivered_ok = dispatch.delivered
    if delivered_ok:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            delivered = dict(state.get("delivered") or {})
            delivered[intent_id] = now.isoformat()
            if len(delivered) > 200:
                delivered = dict(sorted(delivered.items(), key=lambda item: item[1])[-200:])
            state["delivered"] = delivered
            semantic_keys = {
                str(key): str(value)
                for key, value in dict(state.get("semantic_keys") or {}).items()
                if key in delivered
            }
            semantic_key = str(intent.get("semantic_key") or "")
            if semantic_key:
                semantic_keys[intent_id] = semantic_key
            state["semantic_keys"] = semantic_keys
            inflight = dict(state.get("inflight") or {})
            inflight.pop(intent_id, None)
            state["inflight"] = inflight
            state["updated_at"] = now.isoformat()
            atomic_write_json_secure(state_path, state)
        record_push("trade_intent", text, at=now.isoformat())
    else:
        _release_delivery_lease(state_path, intent_id, now=now)
    return {
        "attempted": True,
        "delivered": delivered_ok,
        "writer": writer,
        "sinks": [sink.to_dict() for sink in sinks],
    }


def render_trade_intent(intent: Mapping[str, object]) -> str:
    direction = "向上突破买 Call" if intent.get("direction") == "up" else "向下突破买 Put"
    if intent.get("thesis") == "fade":
        direction = "拒绝下破买 Call" if intent.get("direction") == "up" else "拒绝上破买 Put"
    return "\n".join(
        (
            f"TRADE READY｜{direction}",
            "## 主剧本",
            f"SPX **{_fmt(intent.get('spx_spot'))}**，关键位 **{_fmt(intent.get('trigger_level'))}**，"
            f"确认后延伸 **{_fmt(intent.get('follow_through_points'))} 点**。",
            "## 执行",
            f"**{intent.get('contract_label')}**　实时 `"
            f"{_fmt(intent.get('decision_bid'))} / {_fmt(intent.get('decision_ask'))}`　"
            f"买入上限 `{_fmt(intent.get('entry_limit'))}`",
            f"数据源 {intent.get('provider')}　源时间 {intent.get('quote_source_at')}",
            "## 风险",
            f"SPX 回到 **{_fmt(intent.get('invalidation_spx'))}** 失效　"
            f"目标 **{_fmt(intent.get('target_spx'))}**　"
            f"单张最大权利金 `${_fmt(intent.get('max_loss_per_contract'))}`",
            "## 时效",
            f"意图过期 `{intent.get('expires_at')}`　时间止损 `{intent.get('time_stop_at')}`",
            "自动下单关闭，数量由人工确认。",
        )
    )


def _writer_prompt(intent: Mapping[str, object], template: str) -> str:
    return (
        "把下面已经通过确定性门控的交易意图排成易扫读飞书消息。只做解释和排版，不重新判断。\n"
        f"事实 JSON:\n{json.dumps(dict(intent), ensure_ascii=False, sort_keys=True)}\n"
        f"确定性模板:\n{template}"
    )


def _writer_output_valid(text: str, intent: Mapping[str, object]) -> bool:
    required = (
        str(intent.get("contract_label") or ""),
        _fmt(intent.get("entry_limit")),
        _fmt(intent.get("invalidation_spx")),
        _fmt(intent.get("target_spx")),
    )
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
        )
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]


def _fmt(value: object) -> str:
    if not isinstance(value, int | float):
        return "-"
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


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
