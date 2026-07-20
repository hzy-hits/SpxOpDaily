"""Typed inputs for the live SPXW session accumulator."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.surface_live_session_models import LiveSnapshotError, iso, signed_payload


@dataclass(frozen=True, slots=True)
class LiveInput:
    artifact_sha256: str
    as_of: datetime
    valid_until: datetime
    spot: float
    spot_source_at: datetime
    spot_provider: str
    session_kind: str
    reference_method: str
    frames: Mapping[str, dict[str, object]]
    providers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidatedLiveInput:
    artifact_sha256: str
    as_of: datetime
    created_at: datetime
    valid_until: datetime
    spot: float
    spot_source_at: datetime
    spot_provider: str
    session_kind: str
    reference_method: str
    frame_templates: Mapping[str, dict[str, object]]
    providers: tuple[str, ...]

    def stamp(self, accepted_at: datetime) -> LiveInput:
        accepted = as_utc(accepted_at)
        session_date = DEFAULT_MARKET_CALENDAR.spx_session_date_for(accepted)
        window = (
            DEFAULT_MARKET_CALENDAR.spx_session_window(session_date)
            if session_date is not None
            else None
        )
        if (
            window is None
            or window.segment_at(self.as_of) not in {"gth", "rth"}
            or not self.as_of <= self.created_at <= accepted <= window.session_end
            or not accepted < self.valid_until
        ):
            raise LiveSnapshotError("live_snapshot_acceptance_clock_invalid")
        frames: dict[str, dict[str, object]] = {}
        for role, template in self.frame_templates.items():
            frame = dict(template)
            frame.pop("artifact_sha256", None)
            frame["accepted_at"] = iso(accepted)
            frames[role] = signed_payload(frame)
        return LiveInput(
            artifact_sha256=self.artifact_sha256,
            as_of=self.as_of,
            valid_until=self.valid_until,
            spot=self.spot,
            spot_source_at=self.spot_source_at,
            spot_provider=self.spot_provider,
            session_kind=self.session_kind,
            reference_method=self.reference_method,
            frames=frames,
            providers=self.providers,
        )


__all__ = ("LiveInput", "ValidatedLiveInput")
