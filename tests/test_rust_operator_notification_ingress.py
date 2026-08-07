from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import struct
import threading
import uuid

from spx_spark.application.market_features.virtual_strategy_support import _episode
from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import DeliveryJob
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.notifier.rust_ingress import (
    build_operator_cancellation_message,
    build_operator_ingress_message,
    deliver_operator_notification,
)


NOW = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
TARGETS = (
    ("bark-primary", "bark"),
    ("feishu-primary", "feishu"),
)


def _job(
    *,
    event_id: str = "intent-1:notify:digest",
    kind: str = "trade_intent",
    lane: str = "trade_ready",
    opportunity_id: str = "level-opportunity-1",
    generation: int = 2,
    body: str = (
        "## Desk View\nview\n"
        "## Execution\nexecution\n"
        "## Risk\nrisk\n"
        "## Targets\ntargets\n"
        "## Data Quality\nquality"
    ),
) -> DeliveryJob:
    return DeliveryJob(
        envelope=NotificationEnvelope(
            event_id=event_id,
            source="test",
            kind=kind,
            lane=lane,
            occurred_at=NOW,
            expires_at=NOW + timedelta(minutes=5),
            operator_targets=TARGETS,
            operator_opportunity_id=opportunity_id,
            operator_generation=generation,
        ),
        title="SPX TRADE READY",
        text=body,
        feishu_text=body,
        friend=False,
        targets=("rust_ingress",),
    )


def test_setup_trade_ready_and_virtual_exit_share_lifecycle_identity() -> None:
    episode = _episode(
        source_id="intent-1",
        source_kind="trade_intent",
        direction="up",
        contract_id="SPXW-contract",
        snapshot={"bid": 1.0, "ask": 1.2, "mid": 1.1},
        now=NOW,
        stop=NOW + timedelta(minutes=30),
        invalidation_spx=5500.0,
        target_spx=5530.0,
        invalidation_es=None,
        source_contract={
            "event_id": "level-opportunity-1",
            "reentry_generation": 2,
        },
    )
    assert episode["operator_opportunity_id"] == "level-opportunity-1"
    assert episode["reentry_generation"] == 2

    jobs = (
        _job(
            event_id="level-path:level-opportunity-1:confirmed",
            kind="level_setup_transition",
            lane="market_warning",
        ),
        _job(),
        _job(
            event_id="intent-1:cancel",
            kind="virtual_strategy_exit",
            lane="strategy_lifecycle",
            opportunity_id=str(episode["operator_opportunity_id"]),
            generation=int(episode["reentry_generation"]),
        ),
        _job(
            event_id=f"{episode['episode_id']}:target_reached",
            kind="virtual_strategy_exit",
            lane="strategy_lifecycle",
            opportunity_id=str(episode["operator_opportunity_id"]),
            generation=int(episode["reentry_generation"]),
        ),
    )

    payloads = [
        build_operator_ingress_message(job)["message"]["payload"]
        for job in jobs
    ]
    assert [payload["role"] for payload in payloads] == [
        "setup",
        "trade_ready",
        "cancel",
        "exit",
    ]
    assert {payload["opportunity_id"] for payload in payloads} == {
        "level-opportunity-1"
    }
    assert {payload["generation"] for payload in payloads} == {2}


def test_framed_socket_delivery_preserves_full_body_and_accepts_typed_ack() -> None:
    socket_path = Path("/tmp") / f"spx-rust-ingress-{uuid.uuid4().hex}.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    captured: list[dict[str, object]] = []
    server_errors: list[Exception] = []

    def serve() -> None:
        try:
            connection, _address = listener.accept()
            with connection:
                request_size = struct.unpack(">I", _read_exact(connection, 4))[0]
                request = json.loads(_read_exact(connection, request_size).decode("utf-8"))
                captured.append(request)
                ack = json.dumps(
                    {
                        "schema_version": "spx_core_ack.v1",
                        "status": "accepted",
                        "message_id": request["message_id"],
                        "decision_id": None,
                        "reason_code": "accepted",
                        "disposition": "operator_notification_accepted",
                    },
                    separators=(",", ":"),
                ).encode("utf-8")
                connection.sendall(struct.pack(">I", len(ack)) + ack)
        except Exception as exc:  # pragma: no cover - asserted in main thread
            server_errors.append(exc)
        finally:
            listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    body = "完整正文-" + "方向、入场、失效、目标、数据质量。" * 100
    settings = replace(
        NotificationSettings.from_env(),
        rust_operator_notification_socket_path=str(socket_path),
        rust_operator_notification_timeout_seconds=2.0,
    )
    try:
        result = deliver_operator_notification(settings, _job(body=body))
        thread.join(timeout=3.0)
    finally:
        socket_path.unlink(missing_ok=True)

    assert server_errors == []
    assert thread.is_alive() is False
    assert result.ok is True
    assert result.verdict == (
        "forwarded_to_rust:operator_notification_accepted"
    )
    payload = captured[0]["message"]["payload"]
    assert payload["body"] == body
    assert payload["automatic_ordering"] is False
    assert payload["targets"] == [
        {"channel": "bark", "key": "bark-primary"},
        {"channel": "feishu", "key": "feishu-primary"},
    ]


def test_typed_rejection_classifies_retryable_and_permanent(monkeypatch) -> None:
    settings = NotificationSettings.from_env()
    message_id = str(build_operator_ingress_message(_job())["message_id"])

    monkeypatch.setattr(
        "spx_spark.notifier.rust_ingress._exchange",
        lambda *_args, **_kwargs: {
            "schema_version": "spx_core_ack.v1",
            "status": "rejected",
            "message_id": message_id,
            "decision_id": None,
            "reason_code": "server_busy",
        },
    )
    busy = deliver_operator_notification(settings, _job())
    assert busy.ok is False
    assert busy.permanent is False
    assert busy.error == "rust_ingress_server_busy"

    monkeypatch.setattr(
        "spx_spark.notifier.rust_ingress._exchange",
        lambda *_args, **_kwargs: {
            "schema_version": "spx_core_ack.v1",
            "status": "rejected",
            "message_id": message_id,
            "decision_id": None,
            "reason_code": "invalid_contract_json",
        },
    )
    rejected = deliver_operator_notification(settings, _job())
    assert rejected.ok is False
    assert rejected.permanent is True
    assert rejected.error == "rust_ingress_rejected:invalid_contract_json"

    monkeypatch.setattr(
        "spx_spark.notifier.rust_ingress._exchange",
        lambda *_args, **_kwargs: {
            "schema_version": "spx_core_ack.v1",
            "status": "rejected",
            "message_id": message_id,
            "decision_id": None,
            "reason_code": "processing_rejected",
        },
    )
    processing = deliver_operator_notification(settings, _job())
    assert processing.ok is False
    assert processing.permanent is False
    assert processing.error == "rust_ingress_outcome_unknown:rejected:processing_rejected"


def test_semantic_suppression_is_not_reported_as_forwarded_delivery(monkeypatch) -> None:
    settings = NotificationSettings.from_env()
    message_id = str(build_operator_ingress_message(_job())["message_id"])
    monkeypatch.setattr(
        "spx_spark.notifier.rust_ingress._exchange",
        lambda *_args, **_kwargs: {
            "schema_version": "spx_core_ack.v1",
            "status": "accepted",
            "message_id": message_id,
            "decision_id": None,
            "reason_code": "accepted",
            "disposition": "operator_notification_semantic_suppressed",
        },
    )

    result = deliver_operator_notification(settings, _job())

    assert result.ok is False
    assert result.permanent is True
    assert result.error == "rust_ingress_semantic_suppressed"
    assert result.verdict == "operator_notification_semantic_suppressed"


def test_generic_duplicate_ingress_cannot_hide_operator_suppression(monkeypatch) -> None:
    settings = NotificationSettings.from_env()
    message_id = str(build_operator_ingress_message(_job())["message_id"])
    monkeypatch.setattr(
        "spx_spark.notifier.rust_ingress._exchange",
        lambda *_args, **_kwargs: {
            "schema_version": "spx_core_ack.v1",
            "status": "accepted",
            "message_id": message_id,
            "decision_id": None,
            "reason_code": "accepted",
            "disposition": "duplicate_ingress",
        },
    )

    result = deliver_operator_notification(settings, _job())

    assert result.ok is False
    assert result.permanent is False
    assert result.error == "rust_ingress_outcome_unknown:invalid accepted acknowledgement"


def test_cancellation_envelope_is_stable_and_all_idempotent_acks_succeed(
    monkeypatch,
) -> None:
    settings = NotificationSettings.from_env()
    first = build_operator_cancellation_message(
        event_id="intent-1:notify:digest",
        cancelled_at=NOW,
        reason_code="source_invalidated",
    )
    second = build_operator_cancellation_message(
        event_id="intent-1:notify:digest",
        cancelled_at=NOW,
        reason_code="source_invalidated",
    )
    assert first == second
    assert first["message"] == {
        "kind": "operator_notification_cancellation",
        "payload": {
            "schema_version": "operator_notification_cancellation.v1",
            "event_id": "intent-1:notify:digest",
            "cancelled_at": NOW.isoformat(),
            "reason_code": "source_invalidated",
        },
    }

    from spx_spark.notifier.rust_ingress import (
        deliver_operator_notification_cancellation,
    )

    for disposition in (
        "operator_notification_cancellation_accepted",
        "operator_notification_cancellation_duplicate",
        "duplicate_ingress",
    ):
        monkeypatch.setattr(
            "spx_spark.notifier.rust_ingress._exchange",
            lambda *_args, disposition=disposition, **_kwargs: {
                "schema_version": "spx_core_ack.v1",
                "status": "accepted",
                "message_id": first["message_id"],
                "decision_id": None,
                "reason_code": "accepted",
                "disposition": disposition,
            },
        )
        result = deliver_operator_notification_cancellation(
            settings,
            event_id="intent-1:notify:digest",
            cancelled_at=NOW,
            reason_code="source_invalidated",
        )
        assert result.ok is True
        assert result.verdict == f"rust_cancellation_fenced:{disposition}"


def _read_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    while size:
        chunk = connection.recv(size)
        if not chunk:
            raise OSError("unexpected EOF")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)
