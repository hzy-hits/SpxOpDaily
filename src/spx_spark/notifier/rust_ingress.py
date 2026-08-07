"""Typed Python-to-Rust operator-notification transport."""

from __future__ import annotations

import hashlib
import json
import socket
import struct
from datetime import datetime, timezone
from dataclasses import replace
from typing import Mapping

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox_contract import DeliveryJob
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.receipts import NotificationEnvelope


_OPERATOR_SUCCESS_DISPOSITIONS = frozenset(
    {
        "operator_notification_accepted",
    }
)
_OPERATOR_SEMANTIC_SUPPRESSION = "operator_notification_semantic_suppressed"
_CANCELLATION_SUCCESS_DISPOSITIONS = frozenset(
    {
        "operator_notification_cancellation_accepted",
        "operator_notification_cancellation_duplicate",
        "duplicate_ingress",
    }
)
_PERMANENT_REJECTIONS = frozenset({"invalid_contract_json", "invalid_frame_size"})
_ACK_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "message_id",
        "decision_id",
        "reason_code",
        "disposition",
    }
)


def operator_notification_role(envelope: NotificationEnvelope) -> str | None:
    """Map only the explicitly migrated trader-facing lanes to a closed role."""

    identity = (envelope.kind, envelope.lane)
    if identity == ("level_setup_transition", "market_warning"):
        phase = envelope.event_id.rsplit(":", 1)[-1]
        return "exit" if phase in {"invalidated", "expired"} else "setup"
    if identity == ("gamma_level_prearm_plan", "gamma_prearm_plan"):
        return "setup"
    if identity in {
        ("trade_intent", "trade_ready"),
        ("gth_spxw_manual_spread_candidate", "gth_manual_candidate"),
        (
            "gth_spxw_level_manual_spread_candidate",
            "gth_level_manual_candidate",
        ),
    }:
        return "trade_ready"
    if identity == ("virtual_strategy_exit", "strategy_lifecycle"):
        return "cancel" if envelope.event_id.endswith(":cancel") else "exit"
    return None


def deliver_operator_notification(
    settings: NotificationSettings,
    job: DeliveryJob,
) -> SinkResult:
    """Send one immutable framed request and classify the typed acknowledgement."""

    return deliver_operator_template(
        settings,
        envelope=job.envelope,
        title=job.title,
        text=job.text,
    )


def deliver_operator_template(
    settings: NotificationSettings,
    *,
    envelope: NotificationEnvelope,
    title: str,
    text: str,
) -> SinkResult:
    """Send one final template without requiring the legacy outbox job type."""

    try:
        result = _deliver_message(
            settings,
            build_operator_ingress_message_fields(
                envelope=envelope,
                title=title,
                text=text,
            ),
            success_dispositions=_OPERATOR_SUCCESS_DISPOSITIONS,
        )
        return replace(
            result,
            verdict=(f"forwarded_to_rust:{result.verdict}" if result.ok else result.verdict),
        )
    except ValueError as exc:
        return SinkResult(
            sink="rust_ingress",
            attempted=False,
            ok=False,
            error=f"rust_ingress_contract_error:{exc}",
            permanent=True,
        )
    except (OSError, TimeoutError) as exc:
        return SinkResult(
            sink="rust_ingress",
            attempted=True,
            ok=False,
            error=f"rust_ingress_outcome_unknown:{type(exc).__name__}:{exc}",
            permanent=False,
        )


def deliver_operator_notification_cancellation(
    settings: NotificationSettings,
    *,
    event_id: str,
    cancelled_at: datetime,
    reason_code: str,
) -> SinkResult:
    """Persist a Rust-side pre-event or post-acceptance cancellation fence."""

    try:
        result = _deliver_message(
            settings,
            build_operator_cancellation_message(
                event_id=event_id,
                cancelled_at=cancelled_at,
                reason_code=reason_code,
            ),
            success_dispositions=_CANCELLATION_SUCCESS_DISPOSITIONS,
        )
        return replace(
            result,
            verdict=(f"rust_cancellation_fenced:{result.verdict}" if result.ok else result.verdict),
        )
    except ValueError as exc:
        return SinkResult(
            sink="rust_ingress",
            attempted=False,
            ok=False,
            error=f"rust_ingress_cancellation_contract_error:{exc}",
            permanent=True,
        )
    except (OSError, TimeoutError) as exc:
        return SinkResult(
            sink="rust_ingress",
            attempted=True,
            ok=False,
            error=(f"rust_ingress_cancellation_outcome_unknown:{type(exc).__name__}:{exc}"),
            permanent=False,
        )


def build_operator_ingress_message(job: DeliveryJob) -> dict[str, object]:
    return build_operator_ingress_message_fields(
        envelope=job.envelope,
        title=job.title,
        text=job.text,
    )


def build_operator_ingress_message_fields(
    *,
    envelope: NotificationEnvelope,
    title: str,
    text: str,
) -> dict[str, object]:
    envelope.validate()
    role = operator_notification_role(envelope)
    if role is None:
        raise ValueError("event is not in a Rust-owned trader notification lane")
    if envelope.expires_at is None:
        raise ValueError("operator notification requires expires_at")
    if not envelope.operator_targets:
        raise ValueError("operator notification requires frozen targets")
    _validate_token(envelope.event_id, "event_id")
    _validate_token(title, "title")
    if not text.strip():
        raise ValueError("body is required")
    if "\0" in text:
        raise ValueError("body contains NUL")
    if len(text.encode("utf-8")) > 65_536:
        raise ValueError("body exceeds 65536 UTF-8 bytes")
    opportunity_id = envelope.operator_opportunity_id or _opportunity_id(envelope)
    _validate_token(opportunity_id, "opportunity_id")
    targets = [{"key": key, "channel": channel} for key, channel in envelope.operator_targets]
    for target in targets:
        _validate_token(str(target["key"]), "target key")
    payload: dict[str, object] = {
        "schema_version": "operator_notification.v1",
        "event_id": envelope.event_id,
        "semantic_id": envelope.event_id,
        "opportunity_id": opportunity_id,
        "generation": envelope.operator_generation,
        "role": role,
        "occurred_at": envelope.occurred_at.isoformat(),
        "expires_at": envelope.expires_at.isoformat(),
        "title": title,
        "body": text,
        "targets": targets,
        "automatic_ordering": False,
    }
    canonical_payload = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    message_id = "message:operator:" + hashlib.sha256(canonical_payload).hexdigest()[:24]
    return {
        "schema_version": "spx_ingress.v1",
        "message_id": message_id,
        # This is deliberately immutable across retries. occurred_at is a valid
        # lower bound for the envelope emission time and avoids hash drift.
        "emitted_at": envelope.occurred_at.isoformat(),
        "message": {"kind": "operator_notification", "payload": payload},
    }


def build_operator_cancellation_message(
    *,
    event_id: str,
    cancelled_at: datetime,
    reason_code: str,
) -> dict[str, object]:
    _validate_token(event_id, "event_id")
    _validate_token(reason_code, "reason_code")
    if cancelled_at.tzinfo is None or cancelled_at.utcoffset() is None:
        raise ValueError("cancelled_at must be timezone-aware")
    frozen_at = cancelled_at.astimezone(timezone.utc).isoformat()
    payload: dict[str, object] = {
        "schema_version": "operator_notification_cancellation.v1",
        "event_id": event_id,
        "cancelled_at": frozen_at,
        "reason_code": reason_code,
    }
    canonical_payload = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema_version": "spx_ingress.v1",
        "message_id": "cancel-msg:" + hashlib.sha256(canonical_payload).hexdigest()[:24],
        "emitted_at": frozen_at,
        "message": {
            "kind": "operator_notification_cancellation",
            "payload": payload,
        },
    }


def _deliver_message(
    settings: NotificationSettings,
    message: Mapping[str, object],
    *,
    success_dispositions: frozenset[str],
) -> SinkResult:
    encoded = json.dumps(
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    max_frame_bytes = int(settings.rust_operator_notification_max_frame_bytes)
    if max_frame_bytes < 1 or len(encoded) > max_frame_bytes:
        raise ValueError("rust ingress request exceeds configured frame limit")
    acknowledgement = _exchange(
        settings.rust_operator_notification_socket_path,
        encoded,
        timeout_seconds=float(settings.rust_operator_notification_timeout_seconds),
        max_frame_bytes=max_frame_bytes,
    )
    return _ack_result(
        acknowledgement,
        message_id=str(message["message_id"]),
        success_dispositions=success_dispositions,
    )


def _opportunity_id(envelope: NotificationEnvelope) -> str:
    event_id = envelope.event_id
    if envelope.kind == "trade_intent" and ":notify:" in event_id:
        return event_id.split(":notify:", 1)[0]
    if envelope.lane in {"gth_manual_candidate", "gth_level_manual_candidate"}:
        return event_id.removesuffix(":ready")
    if envelope.kind == "virtual_strategy_exit" and ":" in event_id:
        return event_id.rsplit(":", 1)[0]
    if envelope.kind == "level_setup_transition":
        core = event_id.removeprefix("level-path:")
        return core.rsplit(":", 1)[0] if ":" in core else core
    if envelope.kind == "gamma_level_prearm_plan" and ":" in event_id:
        return event_id.rsplit(":", 1)[0]
    return event_id


def _validate_token(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} is required")
    if "\0" in value:
        raise ValueError(f"{label} contains NUL")
    if len(value.encode("utf-8")) > 4_096:
        raise ValueError(f"{label} exceeds 4096 UTF-8 bytes")


def _exchange(
    path: str,
    payload: bytes,
    *,
    timeout_seconds: float,
    max_frame_bytes: int,
) -> Mapping[str, object]:
    if not path.strip():
        raise ValueError("Rust ingress socket path is required")
    if timeout_seconds <= 0:
        raise ValueError("Rust ingress timeout must be positive")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout_seconds)
        connection.connect(path)
        connection.sendall(struct.pack(">I", len(payload)) + payload)
        header = _read_exact(connection, 4)
        frame_size = struct.unpack(">I", header)[0]
        if frame_size < 1 or frame_size > max_frame_bytes:
            raise OSError("invalid Rust acknowledgement frame size")
        body = _read_exact(connection, frame_size)
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OSError("invalid Rust acknowledgement JSON") from exc
    if not isinstance(decoded, Mapping):
        raise OSError("Rust acknowledgement must be a JSON object")
    return decoded


def _read_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise OSError("Rust ingress closed before acknowledgement completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _ack_result(
    ack: Mapping[str, object],
    *,
    message_id: str,
    success_dispositions: frozenset[str],
) -> SinkResult:
    keys = set(ack)
    status = ack.get("status")
    expected_keys = _ACK_KEYS if status == "accepted" else _ACK_KEYS - {"disposition"}
    if keys != expected_keys:
        return _unknown_ack("invalid acknowledgement fields")
    if ack.get("schema_version") != "spx_core_ack.v1":
        return _unknown_ack("unexpected acknowledgement schema")
    reason = str(ack.get("reason_code") or "")
    acknowledged_id = ack.get("message_id")
    if acknowledged_id not in {None, message_id}:
        return _unknown_ack("acknowledgement message_id mismatch")
    if status == "accepted":
        disposition = ack.get("disposition")
        if (
            acknowledged_id == message_id
            and ack.get("decision_id") is None
            and reason == "accepted"
            and disposition in success_dispositions
        ):
            return SinkResult(
                sink="rust_ingress",
                attempted=True,
                ok=True,
                verdict=str(disposition),
            )
        if (
            acknowledged_id == message_id
            and ack.get("decision_id") is None
            and reason == "accepted"
            and disposition == _OPERATOR_SEMANTIC_SUPPRESSION
        ):
            # Rust durably accepted the ingress frame but deliberately did not
            # create targets.  Treating this as delivery success hides a
            # lifecycle-generation bug and produces a false human receipt.
            return SinkResult(
                sink="rust_ingress",
                attempted=True,
                ok=False,
                error="rust_ingress_semantic_suppressed",
                verdict=str(disposition),
                permanent=True,
            )
        return _unknown_ack("invalid accepted acknowledgement")
    if status != "rejected" or ack.get("decision_id") is not None:
        return _unknown_ack("invalid rejected acknowledgement")
    if reason == "server_busy":
        return SinkResult(
            sink="rust_ingress",
            attempted=True,
            ok=False,
            error="rust_ingress_server_busy",
            verdict=reason,
            permanent=False,
        )
    if reason in _PERMANENT_REJECTIONS:
        return SinkResult(
            sink="rust_ingress",
            attempted=True,
            ok=False,
            error=f"rust_ingress_rejected:{reason}",
            verdict=reason,
            permanent=True,
        )
    return _unknown_ack(f"rejected:{reason or 'missing_reason_code'}")


def _unknown_ack(reason: str) -> SinkResult:
    return SinkResult(
        sink="rust_ingress",
        attempted=True,
        ok=False,
        error=f"rust_ingress_outcome_unknown:{reason}",
        permanent=False,
    )
