"""Validated contracts for the persistent live SPXW session surface."""

from __future__ import annotations

import hmac
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from spx_spark.features.exposure_surface import SurfaceContract
from spx_spark.marketdata import as_utc
from spx_spark.surface_artifact import canonical_sha256
from spx_spark.surface_replay_session_models import _FrameState


LIVE_SERVICE_SCHEMA_VERSION = 1
LIVE_SESSION_STATE_SCHEMA_VERSION = 1
LIVE_SESSION_POLICY_VERSION = "spxw_session_surface.live.v2"
LIVE_SESSION_KIND = "spxw_session_surface"
LIVE_SESSION_MODE = "live"
LIVE_BUCKET_MINUTES = 1
LIVE_PRICE_STEP = 5.0
LIVE_PRICE_EXTENT_POINTS = 100.0
LIVE_TRADING_CLASS = "SPXW"
LIVE_COORDINATE = "SPX"
LIVE_ROLES = ("front", "next")
LIVE_WEIGHTINGS = ("oi_weighted", "volume_weighted")
LIVE_STATUSES = frozenset(
    {"initializing", "ready", "degraded", "lease_expired", "closed", "unavailable"}
)
MAX_LIVE_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_LIVE_STATE_BYTES = 64 * 1024 * 1024
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")


class LiveSessionError(RuntimeError):
    """A live input or persistent state violates the fail-closed contract."""


class LiveSnapshotError(LiveSessionError):
    """The isolated dashboard input cannot be accepted."""


class LiveStateError(LiveSessionError):
    """Persistent live-session state is corrupt or conflicts with frozen state."""


@dataclass(frozen=True, slots=True)
class LiveSelector:
    role: str = "front"
    weighting: str = "oi_weighted"
    bucket_minutes: int = LIVE_BUCKET_MINUTES
    price_step: float = LIVE_PRICE_STEP

    def __post_init__(self) -> None:
        if self.role not in LIVE_ROLES:
            raise ValueError("unsupported live role")
        if self.weighting not in LIVE_WEIGHTINGS:
            raise ValueError("unsupported live weighting")
        if isinstance(self.bucket_minutes, bool) or self.bucket_minutes != LIVE_BUCKET_MINUTES:
            raise ValueError("unsupported live bucket minutes")
        try:
            parsed = float(self.price_step)
        except (TypeError, ValueError) as exc:
            raise ValueError("unsupported live price step") from exc
        if isinstance(self.price_step, bool) or not math.isclose(
            parsed,
            LIVE_PRICE_STEP,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("unsupported live price step")
        object.__setattr__(self, "price_step", parsed)


def finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def nonnegative(value: object) -> float | None:
    parsed = finite(value)
    return parsed if parsed is not None and parsed >= 0 else None


def parse_clock(value: object, *, code: str) -> datetime:
    if not isinstance(value, str):
        raise LiveSnapshotError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LiveSnapshotError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LiveSnapshotError(code)
    return as_utc(parsed)


def iso(value: datetime) -> str:
    return as_utc(value).isoformat()


def mapping(value: object, *, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LiveSnapshotError(code)
    return value


def list_value(value: object, *, code: str) -> list[Any]:
    if not isinstance(value, list):
        raise LiveSnapshotError(code)
    return value


def verify_artifact(payload: Mapping[str, Any], *, code: str) -> str:
    stored = payload.get("artifact_sha256")
    if not isinstance(stored, str) or not _SHA256_RE.fullmatch(stored):
        raise LiveSnapshotError(f"{code}_hash_invalid")
    unsigned = dict(payload)
    unsigned.pop("artifact_sha256", None)
    actual = canonical_sha256(unsigned)
    if not hmac.compare_digest(stored, actual):
        raise LiveSnapshotError(f"{code}_hash_mismatch")
    return stored


def signed_payload(payload: Mapping[str, object]) -> dict[str, object]:
    result = dict(payload)
    result.pop("artifact_sha256", None)
    result["artifact_sha256"] = canonical_sha256(result)
    return result


def _surface_contracts(
    strike_ladder: list[Any],
    *,
    expiry: str,
) -> tuple[tuple[SurfaceContract, ...], tuple[Mapping[str, Any], ...]]:
    contracts: list[SurfaceContract] = []
    rows: list[Mapping[str, Any]] = []
    previous: float | None = None
    for raw in strike_ladder:
        row = mapping(raw, code="live_strike_row_invalid")
        strike = finite(row.get("strike"))
        if strike is None or strike <= 0 or (previous is not None and strike <= previous):
            raise LiveSnapshotError("live_strike_order_invalid")
        for side, right in (("call", "C"), ("put", "P")):
            raw_leg = row.get(side)
            if raw_leg is None:
                continue
            leg = mapping(raw_leg, code="live_strike_leg_invalid")
            contracts.append(
                SurfaceContract(
                    expiry=expiry,
                    strike=strike,
                    right=right,
                    iv=finite(leg.get("iv")),
                    open_interest=nonnegative(leg.get("open_interest")),
                    volume=nonnegative(leg.get("volume")),
                )
            )
        rows.append(dict(row))
        previous = strike
    if not contracts or not rows:
        raise LiveSnapshotError("live_contracts_unavailable")
    return tuple(contracts), tuple(rows)


def frame_state(payload: Mapping[str, Any]) -> _FrameState:
    """Restore one already-validated compact live frame for kernel evaluation."""

    if payload.get("schema_version") != LIVE_SESSION_STATE_SCHEMA_VERSION:
        raise LiveSnapshotError("live_frame_schema_invalid")
    artifact_hash = verify_artifact(payload, code="live_frame")
    role = payload.get("role")
    expiry = payload.get("expiry")
    if role not in LIVE_ROLES or not isinstance(expiry, str) or not re.fullmatch(r"\d{8}", expiry):
        raise LiveSnapshotError("live_frame_identity_invalid")
    accepted_at = parse_clock(payload.get("accepted_at"), code="live_frame_accepted_at_invalid")
    model_as_of = parse_clock(payload.get("model_as_of"), code="live_frame_model_clock_invalid")
    source_at = parse_clock(payload.get("source_at"), code="live_frame_source_clock_invalid")
    input_known_at = parse_clock(payload.get("known_at"), code="live_frame_known_clock_invalid")
    valid_until = parse_clock(payload.get("valid_until"), code="live_frame_lease_invalid")
    expiry_close = parse_clock(payload.get("expiry_close"), code="live_expiry_close_invalid")
    reference_spot = finite(payload.get("reference_spot"))
    if (
        reference_spot is None
        or reference_spot <= 0
        or source_at > model_as_of
        or input_known_at > model_as_of
        or model_as_of > accepted_at
        or valid_until <= accepted_at
    ):
        raise LiveSnapshotError("live_frame_clock_invalid")
    contracts, strike_rows = _surface_contracts(
        list_value(payload.get("strike_ladder"), code="live_strike_ladder_invalid"),
        expiry=expiry,
    )
    raw_warnings = payload.get("warnings")
    warnings = tuple(str(value) for value in raw_warnings) if isinstance(raw_warnings, list) else ()
    session_kind = str(payload.get("session_kind") or "rth")
    if session_kind not in {"gth", "rth"}:
        raise LiveSnapshotError("live_frame_session_kind_invalid")
    frame_providers = payload.get("providers")
    provider_values = (
        sorted({str(value) for value in frame_providers if str(value)})
        if isinstance(frame_providers, list)
        else []
    )
    surface_provider = (
        provider_values[0]
        if len(provider_values) == 1
        else "mixed"
        if provider_values
        else "ibkr" if session_kind == "gth" else "schwab"
    )
    reference_method = str(
        payload.get("reference_method")
        or ("chain_implied" if session_kind == "gth" else "direct_index_spx")
    )
    return _FrameState(
        at=model_as_of,
        valid_until=valid_until,
        artifact_sha256=artifact_hash,
        expiry=expiry,
        expiry_close=expiry_close,
        reference_spot=reference_spot,
        contracts=contracts,
        strike_rows=strike_rows,
        quality=str(payload.get("quality") or "unavailable"),
        warnings=warnings,
        known_at=accepted_at,
        session_kind=session_kind,
        surface_provider=surface_provider,
        reference_method=reference_method,
    )


__all__ = (
    "LIVE_BUCKET_MINUTES",
    "LIVE_COORDINATE",
    "LIVE_PRICE_EXTENT_POINTS",
    "LIVE_PRICE_STEP",
    "LIVE_ROLES",
    "LIVE_SERVICE_SCHEMA_VERSION",
    "LIVE_SESSION_KIND",
    "LIVE_SESSION_MODE",
    "LIVE_SESSION_POLICY_VERSION",
    "LIVE_SESSION_STATE_SCHEMA_VERSION",
    "LIVE_STATUSES",
    "LIVE_TRADING_CLASS",
    "LIVE_WEIGHTINGS",
    "LiveSelector",
    "LiveSessionError",
    "LiveSnapshotError",
    "LiveStateError",
    "MAX_LIVE_SNAPSHOT_BYTES",
    "MAX_LIVE_STATE_BYTES",
    "finite",
    "frame_state",
    "iso",
    "list_value",
    "mapping",
    "nonnegative",
    "parse_clock",
    "signed_payload",
    "verify_artifact",
)
