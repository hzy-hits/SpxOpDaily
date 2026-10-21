from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.clock import FixedClock
from spx_spark.domain.payload_schemas import (
    PayloadSchemaError,
    UnknownPayloadVersion,
    payload_json_schema,
    validate_strategy_decision,
)
from spx_spark.domain.protocol import Envelope, envelope_for

NOW = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)


def _decision() -> dict[str, object]:
    return {
        "schema_version": "strategy_decision.v2",
        "decision_id": "strategy:schema",
        "policy_version": "v69",
        "decision_at": NOW.isoformat(),
        "available_at": NOW.isoformat(),
        "decision_type": "NO_TRADE",
        "automatic_ordering": False,
        "correlation_id": "corr:schema",
        "candidate": None,
    }


def _notification() -> dict[str, object]:
    return {
        "message_id": "msg:schema",
        "message_class": "ready",
        "lane": "trade_ready",
        "as_of": NOW.isoformat(),
        "expires_at": "2026-10-16T14:05:00+00:00",
        "subject": {"structure": "CALL_DEBIT_VERTICAL"},
        "decision": {"decision_id": "strategy:schema"},
        "provenance": {"correlation_id": "corr:schema"},
        "template_id": "trade_ready",
        "template_version": 1,
        "correlation_id": "corr:schema",
    }


def test_payload_schemas_fail_closed_and_the_envelope_validates() -> None:
    parsed = validate_strategy_decision(_decision())
    assert parsed.decision_id == "strategy:schema"

    with pytest.raises(UnknownPayloadVersion):
        validate_strategy_decision({**_decision(), "schema_version": "strategy_decision.v9"})
    with pytest.raises(PayloadSchemaError):
        validate_strategy_decision({**_decision(), "policy_version": ""})

    envelope = envelope_for(
        message_type="notification_intent",
        version=1,
        as_of=NOW,
        source="test",
        correlation_id="corr:schema",
        payload=_notification(),
        clock=FixedClock(NOW),
    )
    assert envelope.produced_at == NOW
    assert envelope.validated_payload().message_class == "ready"
    assert "message_class" in payload_json_schema("notification_intent", 1)["properties"]

    with pytest.raises(UnknownPayloadVersion):
        Envelope(
            message_id="msg:bad",
            type="notification_intent",
            version=99,
            produced_at=NOW,
            as_of=NOW,
            source="test",
            correlation_id="corr:bad",
            payload=_notification(),
        ).validated_payload()
