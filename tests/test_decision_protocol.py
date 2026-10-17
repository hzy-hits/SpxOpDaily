from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.domain.protocol import Envelope, envelope_for
from spx_spark.notifier.model import NotificationEnvelope


def test_decision_envelope_carries_one_utc_correlation_id() -> None:
    as_of = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    envelope = envelope_for(
        message_type="strategy_decision",
        version=1,
        as_of=as_of,
        source="core",
        correlation_id="corr:test",
        payload={"status": "no_trade"},
        produced_at=as_of,
    )

    payload = envelope.to_dict()
    assert payload["correlation_id"] == "corr:test"
    assert payload["as_of"] == as_of.isoformat()
    assert payload["payload"] == {"status": "no_trade"}

    with pytest.raises(ValueError):
        Envelope(
            message_id="msg:test",
            type="strategy_decision",
            version=1,
            produced_at=datetime(2026, 10, 16, 14, 0),
            as_of=as_of,
            source="core",
            correlation_id="corr:test",
        )
    blank = NotificationEnvelope(
        event_id="evt",
        source="core",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=as_of,
        correlation_id="   ",
    )
    with pytest.raises(ValueError):
        blank.validate()

def test_notification_envelope_maps_lane_to_message_class() -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    envelope = NotificationEnvelope(
        event_id="evt",
        source="core",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=now,
    )
    assert envelope.effective_message_class == "ready"
    envelope.validate()
    invalid = NotificationEnvelope(
        event_id="evt",
        source="core",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=now,
        message_class="definitely_not_a_class",
    )
    with pytest.raises(ValueError):
        invalid.validate()
