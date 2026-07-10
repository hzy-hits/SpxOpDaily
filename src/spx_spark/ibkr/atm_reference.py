from __future__ import annotations

import json
import math
import os
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


STATE_SCHEMA_VERSION = 1
BASIS_WINDOW_SECONDS = 5 * 60
BASIS_MIN_SAMPLES = 5
BASIS_MIN_SPAN_SECONDS = 30.0
BASIS_MAX_TIMESTAMP_SKEW_SECONDS = 5.0
BASIS_MAX_ABS_POINTS = 120.0
BASIS_MAX_MEDIAN_DEVIATION_POINTS = 15.0
BASIS_MAX_TRADING_DAY_AGE = 3


@dataclass(frozen=True)
class ReferenceQuote:
    """One normalized cash or proxy observation used for ATM selection."""

    value: float | None
    observed_at: datetime | None
    freshness: str
    contract: str | None = None

    @property
    def is_fresh(self) -> bool:
        return (
            self.freshness == "fresh"
            and self.observed_at is not None
            and _valid_price(self.value)
        )


@dataclass(frozen=True)
class BasisObservation:
    value: float
    observed_at: datetime


@dataclass(frozen=True)
class BasisState:
    es_contract: str
    trading_date: date
    samples: tuple[BasisObservation, ...]
    sample_count: int
    sample_window_start: datetime | None
    median: float | None
    observed_at: datetime | None

    @property
    def is_qualified(self) -> bool:
        return self.median is not None


@dataclass(frozen=True)
class StableAtmState:
    rounded_strike: int
    source: str
    observed_at: datetime
    expiry: str | None = None


@dataclass(frozen=True)
class AtmReferenceCandidate:
    value: float
    rounded_strike: int
    source: str
    observed_at: datetime
    freshness: str
    basis_value: float | None
    basis_as_of: datetime | None
    basis_contract: str | None
    reason: str


@dataclass(frozen=True)
class AtmReferenceResult:
    candidate: AtmReferenceCandidate | None
    stable_atm: StableAtmState | None
    basis: BasisState | None
    reason: str


class EsSpxBasisTracker:
    """Build a robust ES-minus-SPX basis from synchronized RTH observations."""

    def __init__(self, state: BasisState | None = None) -> None:
        self.state = state

    def observe(
        self,
        *,
        spx: ReferenceQuote | None,
        es: ReferenceQuote | None,
        is_rth: bool,
        trading_date: date,
    ) -> BasisState | None:
        if es is not None and es.contract:
            self.invalidate_for_contract(es.contract)
        if not is_rth or spx is None or es is None:
            return self.state
        if not spx.is_fresh or not es.is_fresh or not es.contract:
            return self.state

        spx_at = _as_utc(spx.observed_at)
        es_at = _as_utc(es.observed_at)
        if abs((es_at - spx_at).total_seconds()) > BASIS_MAX_TIMESTAMP_SKEW_SECONDS:
            return self.state

        basis_value = float(es.value) - float(spx.value)
        if abs(basis_value) > BASIS_MAX_ABS_POINTS:
            return self.state

        observed_at = max(spx_at, es_at)
        existing = self.state
        if (
            existing is None
            or existing.es_contract != es.contract
            or existing.trading_date != trading_date
        ):
            samples: tuple[BasisObservation, ...] = ()
        else:
            samples = tuple(
                sample
                for sample in existing.samples
                if (observed_at - sample.observed_at).total_seconds()
                <= BASIS_WINDOW_SECONDS
            )

        # Persistent IBKR ticker objects are sampled repeatedly. Count a source
        # observation pair once so a motionless quote cannot qualify a basis.
        if samples and observed_at <= samples[-1].observed_at:
            return self.state

        if samples:
            current_median = statistics.median(sample.value for sample in samples)
            if abs(basis_value - current_median) > BASIS_MAX_MEDIAN_DEVIATION_POINTS:
                return self.state

        samples = (*samples, BasisObservation(basis_value, observed_at))
        sample_span = (samples[-1].observed_at - samples[0].observed_at).total_seconds()
        median = (
            float(statistics.median(sample.value for sample in samples))
            if len(samples) >= BASIS_MIN_SAMPLES and sample_span >= BASIS_MIN_SPAN_SECONDS
            else None
        )
        self.state = BasisState(
            es_contract=es.contract,
            trading_date=trading_date,
            samples=samples,
            sample_count=len(samples),
            sample_window_start=samples[0].observed_at,
            median=median,
            observed_at=observed_at,
        )
        return self.state

    def invalidate_for_contract(self, es_contract: str | None) -> bool:
        if self.state is None or not es_contract or self.state.es_contract == es_contract:
            return False
        self.state = None
        return True

    def valid_state(
        self,
        *,
        es_contract: str | None,
        trading_days_since_observation: int | None,
    ) -> BasisState | None:
        state = self.state
        if state is None or not state.is_qualified:
            return None
        if not es_contract or state.es_contract != es_contract:
            return None
        if trading_days_since_observation is None:
            return None
        if not 0 <= trading_days_since_observation <= BASIS_MAX_TRADING_DAY_AGE:
            return None
        return state


class AtmReferenceController:
    """Choose a provenance-carrying SPX ATM reference and persist stable state."""

    def __init__(self, state_path: Path | None = None) -> None:
        self.state_path = Path(state_path) if state_path is not None else None
        basis, stable_atm, stale_bootstrap_used = _load_state(self.state_path)
        self.basis_tracker = EsSpxBasisTracker(basis)
        self.stable_atm = stable_atm
        self.stale_spx_bootstrap_used = stale_bootstrap_used

    def resolve(
        self,
        *,
        strike_step: int,
        is_rth: bool,
        trading_date: date,
        trading_days_since_basis: int | None,
        spx: ReferenceQuote | None = None,
        ibus500: ReferenceQuote | None = None,
        es: ReferenceQuote | None = None,
        spy: ReferenceQuote | None = None,
        expiry_rollover: bool = False,
    ) -> AtmReferenceResult:
        if strike_step <= 0:
            raise ValueError("strike_step must be positive")

        previous_basis = self.basis_tracker.state
        self.basis_tracker.observe(
            spx=spx,
            es=es,
            is_rth=is_rth,
            trading_date=trading_date,
        )

        es_contract = es.contract if es is not None else None
        valid_basis = self.basis_tracker.valid_state(
            es_contract=es_contract,
            trading_days_since_observation=trading_days_since_basis,
        )

        candidate: AtmReferenceCandidate | None = None
        if is_rth and spx is not None and spx.is_fresh:
            candidate = _candidate(
                quote=spx,
                value=float(spx.value),
                strike_step=strike_step,
                source="SPX",
                reason="rth_fresh_spx_authoritative",
            )
        elif not is_rth and ibus500 is not None and ibus500.is_fresh:
            candidate = _candidate(
                quote=ibus500,
                value=float(ibus500.value),
                strike_step=strike_step,
                source="IBUS500",
                reason="off_hours_fresh_cash_proxy",
            )
        elif es is not None and es.is_fresh and valid_basis is not None:
            candidate = _candidate(
                quote=es,
                value=float(es.value) - float(valid_basis.median),
                strike_step=strike_step,
                source="ES_basis_adj",
                reason="fresh_es_with_persisted_rth_basis",
                basis=valid_basis,
            )
        elif spy is not None and spy.is_fresh:
            candidate = _candidate(
                quote=spy,
                value=float(spy.value) * 10.0,
                strike_step=strike_step,
                source="SPY*10",
                reason="fresh_spy_cash_proxy",
            )
        elif expiry_rollover and self.stable_atm is not None:
            stable = self.stable_atm
            candidate = AtmReferenceCandidate(
                value=float(stable.rounded_strike),
                rounded_strike=stable.rounded_strike,
                source="stable_atm",
                observed_at=stable.observed_at,
                freshness="stable",
                basis_value=None,
                basis_as_of=None,
                basis_contract=None,
                reason="expiry_rollover_stable_atm",
            )
        elif (
            self.stable_atm is None
            and not self.stale_spx_bootstrap_used
            and spx is not None
            and spx.freshness == "stale"
            and _valid_price(spx.value)
            and spx.observed_at is not None
        ):
            candidate = _candidate(
                quote=spx,
                value=float(spx.value),
                strike_step=strike_step,
                source="SPX_stale_bootstrap",
                reason="one_time_stale_spx_bootstrap",
            )

        if previous_basis != self.basis_tracker.state:
            self._persist()

        reason = candidate.reason if candidate is not None else "no_eligible_reference"
        return AtmReferenceResult(
            candidate=candidate,
            stable_atm=self.stable_atm,
            basis=valid_basis,
            reason=reason,
        )

    def record_accepted(
        self,
        candidate: AtmReferenceCandidate,
        *,
        expiry: str | None = None,
    ) -> None:
        if candidate.source == "SPX_stale_bootstrap":
            # Consume the one-shot fallback only after its subscription plan
            # has been installed.  A failed reconcile may then retry safely.
            self.stale_spx_bootstrap_used = True
        self.stable_atm = StableAtmState(
            rounded_strike=candidate.rounded_strike,
            source=candidate.source,
            observed_at=_as_utc(candidate.observed_at),
            expiry=expiry,
        )
        self._persist()

    def _persist(self) -> None:
        if self.state_path is None:
            return
        payload = _state_payload(
            basis=self.basis_tracker.state,
            stable_atm=self.stable_atm,
            stale_spx_bootstrap_used=self.stale_spx_bootstrap_used,
        )
        with exclusive_state_lock(self.state_path):
            atomic_write_json_secure(self.state_path, payload)


def _candidate(
    *,
    quote: ReferenceQuote,
    value: float,
    strike_step: int,
    source: str,
    reason: str,
    basis: BasisState | None = None,
) -> AtmReferenceCandidate:
    return AtmReferenceCandidate(
        value=value,
        rounded_strike=_round_strike(value, strike_step),
        source=source,
        observed_at=_as_utc(quote.observed_at),
        freshness=quote.freshness,
        basis_value=basis.median if basis is not None else None,
        basis_as_of=basis.observed_at if basis is not None else None,
        basis_contract=basis.es_contract if basis is not None else None,
        reason=reason,
    )


def _round_strike(value: float, strike_step: int) -> int:
    return int(math.floor(value / strike_step + 0.5)) * strike_step


def _valid_price(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


def _as_utc(value: datetime | None) -> datetime:
    if value is None:
        raise ValueError("observation timestamp is required")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _state_payload(
    *,
    basis: BasisState | None,
    stable_atm: StableAtmState | None,
    stale_spx_bootstrap_used: bool,
) -> dict[str, object]:
    basis_payload: dict[str, object] | None = None
    if basis is not None:
        basis_payload = {
            "es_contract": basis.es_contract,
            "trading_date": basis.trading_date.isoformat(),
            "samples": [
                {"value": sample.value, "observed_at": sample.observed_at.isoformat()}
                for sample in basis.samples
            ],
            "sample_count": basis.sample_count,
            "sample_window_start": (
                basis.sample_window_start.isoformat()
                if basis.sample_window_start is not None
                else None
            ),
            "median": basis.median,
            "observed_at": basis.observed_at.isoformat() if basis.observed_at else None,
        }
    stable_payload: dict[str, object] | None = None
    if stable_atm is not None:
        stable_payload = {
            "rounded_strike": stable_atm.rounded_strike,
            "source": stable_atm.source,
            "observed_at": stable_atm.observed_at.isoformat(),
            "expiry": stable_atm.expiry,
        }
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "basis": basis_payload,
        "stable_atm": stable_payload,
        "stale_spx_bootstrap_used": stale_spx_bootstrap_used,
    }


def _load_state(
    path: Path | None,
) -> tuple[BasisState | None, StableAtmState | None, bool]:
    if path is None or not path.exists():
        return None, None, False
    try:
        with exclusive_state_lock(path):
            raw = json.loads(path.read_text(encoding="utf-8"))
            os.chmod(path, 0o600)
        if not isinstance(raw, dict) or raw.get("schema_version") != STATE_SCHEMA_VERSION:
            raise ValueError("unsupported ATM state schema")
        basis = _parse_basis(raw.get("basis"))
        stable_atm = _parse_stable_atm(raw.get("stable_atm"))
        stale_bootstrap_used = raw.get("stale_spx_bootstrap_used", False)
        if not isinstance(stale_bootstrap_used, bool):
            raise ValueError("invalid stale bootstrap marker")
        return basis, stable_atm, stale_bootstrap_used
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None, None, False


def _parse_basis(raw: object) -> BasisState | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("invalid basis state")
    raw_samples = raw.get("samples")
    if not isinstance(raw_samples, list):
        raise ValueError("invalid basis samples")
    samples: list[BasisObservation] = []
    for item in raw_samples:
        if not isinstance(item, dict):
            raise ValueError("invalid basis sample")
        value = item.get("value")
        observed_at = item.get("observed_at")
        if not isinstance(value, int | float) or not math.isfinite(float(value)):
            raise ValueError("invalid basis value")
        samples.append(
            BasisObservation(float(value), _parse_datetime(observed_at))
        )
    if not samples:
        raise ValueError("basis state requires sample evidence")
    for index, (previous, current) in enumerate(
        zip(samples, samples[1:]),
        start=1,
    ):
        if current.observed_at <= previous.observed_at:
            raise ValueError("basis samples must be strictly increasing")
        prior_median = statistics.median(sample.value for sample in samples[:index])
        if abs(current.value - prior_median) > BASIS_MAX_MEDIAN_DEVIATION_POINTS:
            raise ValueError("basis sample violates median deviation bound")
    if any(abs(sample.value) > BASIS_MAX_ABS_POINTS for sample in samples):
        raise ValueError("basis sample exceeds absolute bound")
    sample_span = (samples[-1].observed_at - samples[0].observed_at).total_seconds()
    if sample_span > BASIS_WINDOW_SECONDS:
        raise ValueError("basis sample window is too wide")
    es_contract = raw.get("es_contract")
    trading_date_raw = raw.get("trading_date")
    if not isinstance(es_contract, str) or not es_contract:
        raise ValueError("invalid ES contract")
    if not isinstance(trading_date_raw, str):
        raise ValueError("invalid basis trading date")
    parsed_date = date.fromisoformat(trading_date_raw)
    median_raw = raw.get("median")
    median = None
    if median_raw is not None:
        if not isinstance(median_raw, int | float) or not math.isfinite(float(median_raw)):
            raise ValueError("invalid basis median")
        median = float(median_raw)
    observed_at_raw = raw.get("observed_at")
    window_start_raw = raw.get("sample_window_start")
    state = BasisState(
        es_contract=es_contract,
        trading_date=parsed_date,
        samples=tuple(samples),
        sample_count=len(samples),
        sample_window_start=(
            _parse_datetime(window_start_raw) if window_start_raw is not None else None
        ),
        median=median,
        observed_at=(
            _parse_datetime(observed_at_raw) if observed_at_raw is not None else None
        ),
    )
    # Persisted summaries must agree with the evidence they summarize.
    if raw.get("sample_count") != state.sample_count:
        raise ValueError("basis sample count mismatch")
    if samples and state.sample_window_start != samples[0].observed_at:
        raise ValueError("basis window mismatch")
    if state.observed_at != samples[-1].observed_at:
        raise ValueError("basis observation timestamp mismatch")
    qualified = len(samples) >= BASIS_MIN_SAMPLES and sample_span >= BASIS_MIN_SPAN_SECONDS
    expected_median = float(statistics.median(sample.value for sample in samples))
    if qualified:
        if state.median is None or not math.isclose(
            state.median,
            expected_median,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError("basis median does not match sample evidence")
    elif state.median is not None:
        raise ValueError("unqualified basis cannot contain a median")
    return state


def _parse_stable_atm(raw: object) -> StableAtmState | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("invalid stable ATM state")
    rounded_strike = raw.get("rounded_strike")
    source = raw.get("source")
    expiry = raw.get("expiry")
    if not isinstance(rounded_strike, int) or rounded_strike <= 0:
        raise ValueError("invalid stable ATM strike")
    if not isinstance(source, str) or not source:
        raise ValueError("invalid stable ATM source")
    if expiry is not None and (not isinstance(expiry, str) or not expiry.isdigit()):
        raise ValueError("invalid stable ATM expiry")
    return StableAtmState(
        rounded_strike=rounded_strike,
        source=source,
        observed_at=_parse_datetime(raw.get("observed_at")),
        expiry=expiry,
    )


def _parse_datetime(raw: object) -> datetime:
    if not isinstance(raw, str):
        raise ValueError("invalid timestamp")
    return _as_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
