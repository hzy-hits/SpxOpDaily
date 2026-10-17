"""Versioned payload schemas for the cross-process envelope.

The three payloads that cross process boundaries (strategy decision, trade
intent, notification intent) are validated here. Unknown type/version pairs fail
closed; extra fields stay allowed so a producer can add evidence without a
schema bump, while the contract fields remain typed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PayloadSchemaError(ValueError):
    """A payload does not satisfy its declared type/version."""


class UnknownPayloadVersion(PayloadSchemaError):
    """No schema is registered for this type/version; consumers must fail closed."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("timestamp must be timezone-aware UTC")
    return value


class _Contract(BaseModel):
    model_config = ConfigDict(extra="allow")


class StrategyDecisionPayload(_Contract):
    schema_version: Literal["strategy_decision.v2"]
    decision_id: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    decision_at: datetime
    available_at: datetime
    decision_type: str = Field(min_length=1)
    automatic_ordering: bool = False
    correlation_id: str | None = None
    session_date: str | None = None
    candidate: dict[str, Any] | None = None

    @field_validator("decision_at", "available_at")
    @classmethod
    def _timestamps_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("automatic_ordering")
    @classmethod
    def _never_automatic(cls, value: bool) -> bool:
        if value:
            raise ValueError("automatic_ordering must be false")
        return value


class StrategyDecisionV1Payload(StrategyDecisionPayload):
    schema_version: Literal["strategy_decision.v1"]


class TradeIntentPayload(_Contract):
    schema_version: Literal["trade_intent.v1"] = "trade_intent.v1"
    event_id: str = Field(min_length=1)
    opportunity_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    structure: str = Field(min_length=1)
    direction: str = Field(min_length=1)
    legs: list[dict[str, Any]] = Field(default_factory=list)
    entry_basis: dict[str, Any] | None = None
    max_loss: float | None = None
    invalidation: dict[str, Any] | None = None
    expires_at: datetime
    template_id: str | None = None
    template_version: int = 1

    @field_validator("expires_at")
    @classmethod
    def _expires_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("template_version")
    @classmethod
    def _template_version(cls, value: int) -> int:
        if value < 1:
            raise ValueError("template_version must be >= 1")
        return value


MessageClass = Literal[
    "watch",
    "armed",
    "ready",
    "no_trade",
    "execution",
    "management",
    "exit",
    "post_mortem",
    "ops",
    "data_quality",
]


class NotificationIntentPayload(_Contract):
    schema_version: Literal["notification_intent.v1"] = "notification_intent.v1"
    message_id: str = Field(min_length=1)
    message_class: MessageClass
    lane: str = Field(min_length=1)
    severity: str = "info"
    as_of: datetime
    expires_at: datetime | None = None
    subject: dict[str, Any] = Field(default_factory=dict)
    decision: dict[str, Any] = Field(default_factory=dict)
    economics: dict[str, Any] | None = None
    risk: dict[str, Any] | None = None
    data_quality: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, Any] = Field(default_factory=dict)
    narrative: str | None = None
    template_id: str | None = None
    template_version: int = 1
    correlation_id: str | None = None

    @field_validator("as_of", "expires_at")
    @classmethod
    def _timestamps_utc(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None

    @field_validator("template_version")
    @classmethod
    def _template_version(cls, value: int) -> int:
        if value < 1:
            raise ValueError("template_version must be >= 1")
        return value

    @model_validator(mode="after")
    def _expires_after_as_of(self) -> "NotificationIntentPayload":
        if self.expires_at is not None and self.expires_at <= self.as_of:
            raise ValueError("expires_at must be later than as_of")
        return self


_SCHEMAS: dict[tuple[str, int], type[BaseModel]] = {
    ("strategy_decision", 1): StrategyDecisionV1Payload,
    ("strategy_decision", 2): StrategyDecisionPayload,
    ("trade_intent", 1): TradeIntentPayload,
    ("notification_intent", 1): NotificationIntentPayload,
}


def payload_schema(message_type: str, version: int) -> type[BaseModel]:
    try:
        return _SCHEMAS[(message_type, int(version))]
    except (KeyError, TypeError, ValueError) as exc:
        raise UnknownPayloadVersion(
            f"unknown payload schema: {message_type}.v{version}"
        ) from exc


def validate_payload(
    message_type: str,
    version: int,
    payload: Mapping[str, Any],
) -> BaseModel:
    schema = payload_schema(message_type, version)
    try:
        return schema.model_validate(dict(payload))
    except Exception as exc:  # pydantic.ValidationError, but keep the domain error
        raise PayloadSchemaError(
            f"invalid {message_type}.v{version} payload: {exc}"
        ) from exc


def validate_strategy_decision(decision: Mapping[str, Any]) -> BaseModel:
    version = {"strategy_decision.v1": 1, "strategy_decision.v2": 2}.get(
        str(decision.get("schema_version") or "")
    )
    if version is None:
        raise UnknownPayloadVersion(
            f"unknown strategy decision version: {decision.get('schema_version')}"
        )
    return validate_payload("strategy_decision", version, decision)


def payload_json_schema(message_type: str, version: int) -> dict[str, Any]:
    return payload_schema(message_type, version).model_json_schema()
