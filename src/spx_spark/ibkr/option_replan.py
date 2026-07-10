from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class ReplanProposal:
    atm_strike: int
    source: str
    expiry: str
    observed_at: datetime
    reason: str
    confirmation_count: int


@dataclass(frozen=True)
class ReplanDecision:
    proposal: ReplanProposal | None
    state: str
    reason: str
    confirmation_count: int = 0


@dataclass
class OptionReplanController:
    trigger_points: float = 20.0
    rearm_points: float = 10.0
    confirmation_count: int = 3
    confirmation_span_seconds: float = 15.0
    cooldown_seconds: float = 120.0
    emergency_points: float = 40.0
    emergency_confirmation_count: int = 2
    emergency_span_seconds: float = 5.0
    hard_minimum_seconds: float = 30.0
    source_grace_seconds: float = 30.0
    failure_backoff_seconds: float = 30.0
    accepted_atm: int | None = None
    accepted_source: str | None = None
    accepted_expiry: str | None = None
    last_applied_at: datetime | None = None
    last_candidate_at: datetime | None = None
    pending_atm: int | None = None
    pending_source: str | None = None
    pending_first_at: datetime | None = None
    pending_last_at: datetime | None = None
    pending_count: int = 0
    failed_until: datetime | None = None
    failed_expiry: str | None = None

    def observe(
        self,
        *,
        atm_strike: int | None,
        source: str | None,
        observed_at: datetime,
        expiry: str,
        decision_at: datetime | None = None,
    ) -> ReplanDecision:
        observed_at = _as_utc(observed_at)
        decision_at = _as_utc(decision_at or observed_at)
        if (
            atm_strike is not None
            and source == self.accepted_source
            and (self.last_candidate_at is None or observed_at > self.last_candidate_at)
        ):
            # Backoff suppresses rebuilds, not liveness evidence for the
            # accepted source.  Retaining these ticks preserves source grace.
            self.last_candidate_at = observed_at
        if (
            self.failed_until is not None
            and decision_at < self.failed_until
            and expiry == self.failed_expiry
        ):
            return ReplanDecision(None, "cooldown", "failure_backoff")
        if self.accepted_atm is None or self.accepted_expiry is None:
            if atm_strike is None:
                return ReplanDecision(None, "steady", "initial_reference_missing")
            proposal = ReplanProposal(
                atm_strike=atm_strike,
                source=source or "unknown",
                expiry=expiry,
                observed_at=observed_at,
                reason="initial_plan",
                confirmation_count=1,
            )
            return ReplanDecision(proposal, "pending", "initial_plan", 1)

        if expiry != self.accepted_expiry:
            rollover_atm = atm_strike if atm_strike is not None else self.accepted_atm
            rollover_source = source or self.accepted_source or "stable_atm"
            proposal = ReplanProposal(
                atm_strike=rollover_atm,
                source=rollover_source,
                expiry=expiry,
                observed_at=observed_at,
                reason="expiry_rollover",
                confirmation_count=1,
            )
            return ReplanDecision(proposal, "pending", "expiry_rollover", 1)

        if atm_strike is None:
            if self.last_candidate_at is not None:
                gap = (observed_at - self.last_candidate_at).total_seconds()
                if gap <= self.source_grace_seconds:
                    return ReplanDecision(
                        None,
                        self._state_at(decision_at),
                        "source_grace",
                        self.pending_count,
                    )
            self._clear_pending()
            return ReplanDecision(None, self._state_at(decision_at), "reference_missing")

        if source != self.accepted_source and self.last_candidate_at is not None:
            gap = (decision_at - self.last_candidate_at).total_seconds()
            if gap <= self.source_grace_seconds:
                self._clear_pending()
                return ReplanDecision(None, self._state_at(decision_at), "source_grace")
        drift = abs(atm_strike - self.accepted_atm)
        if drift <= self.rearm_points:
            self._clear_pending()
            return ReplanDecision(None, self._state_at(decision_at), "inside_rearm_band")
        if drift < self.trigger_points:
            self._clear_pending()
            return ReplanDecision(None, self._state_at(decision_at), "inside_trigger_band")

        since_applied = (
            float("inf")
            if self.last_applied_at is None
            else (decision_at - self.last_applied_at).total_seconds()
        )
        emergency = drift >= self.emergency_points and since_applied >= self.hard_minimum_seconds
        normal = since_applied >= self.cooldown_seconds
        if not emergency and not normal:
            self._clear_pending()
            return ReplanDecision(None, "cooldown", "cooldown_active")

        key_changed = atm_strike != self.pending_atm or (source or "unknown") != self.pending_source
        if key_changed:
            self.pending_atm = atm_strike
            self.pending_source = source or "unknown"
            self.pending_first_at = observed_at
            self.pending_last_at = observed_at
            self.pending_count = 1
        elif self.pending_last_at is None or observed_at > self.pending_last_at:
            self.pending_last_at = observed_at
            self.pending_count += 1

        required_count = (
            self.emergency_confirmation_count if emergency else self.confirmation_count
        )
        required_span = (
            self.emergency_span_seconds if emergency else self.confirmation_span_seconds
        )
        span = (
            0.0
            if self.pending_first_at is None or self.pending_last_at is None
            else (self.pending_last_at - self.pending_first_at).total_seconds()
        )
        if self.pending_count < required_count or span < required_span:
            return ReplanDecision(
                None,
                "pending",
                "awaiting_confirmations",
                self.pending_count,
            )

        reason = "emergency_move" if emergency else "confirmed_move"
        proposal = ReplanProposal(
            atm_strike=atm_strike,
            source=source or "unknown",
            expiry=expiry,
            observed_at=observed_at,
            reason=reason,
            confirmation_count=self.pending_count,
        )
        return ReplanDecision(proposal, "pending", reason, self.pending_count)

    def record_result(
        self,
        proposal: ReplanProposal,
        *,
        success: bool,
        applied_at: datetime | None = None,
    ) -> None:
        decision_at = _as_utc(applied_at or proposal.observed_at)
        if success:
            self.accepted_atm = proposal.atm_strike
            self.accepted_source = proposal.source
            self.accepted_expiry = proposal.expiry
            self.last_applied_at = decision_at
            self.last_candidate_at = _as_utc(proposal.observed_at)
            self.failed_until = None
            self.failed_expiry = None
        else:
            self.failed_until = decision_at + timedelta(
                seconds=max(self.failure_backoff_seconds, 0.0)
            )
            self.failed_expiry = proposal.expiry
        self._clear_pending()

    def _state_at(self, observed_at: datetime) -> str:
        if self.last_applied_at is None:
            return "steady"
        elapsed = (observed_at - self.last_applied_at).total_seconds()
        return "cooldown" if elapsed < self.cooldown_seconds else "steady"

    def _clear_pending(self) -> None:
        self.pending_atm = None
        self.pending_source = None
        self.pending_first_at = None
        self.pending_last_at = None
        self.pending_count = 0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
