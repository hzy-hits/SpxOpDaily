from __future__ import annotations

import json
import stat
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.ibkr.atm_reference import (
    AtmReferenceController,
    EsSpxBasisTracker,
    ReferenceQuote,
)


NOW = datetime(2026, 7, 9, 14, 30, tzinfo=timezone.utc)
TRADING_DATE = date(2026, 7, 9)


def quote(
    value: float,
    *,
    seconds: float = 0,
    freshness: str = "fresh",
    contract: str | None = None,
) -> ReferenceQuote:
    return ReferenceQuote(
        value=value,
        observed_at=NOW + timedelta(seconds=seconds),
        freshness=freshness,
        contract=contract,
    )


def resolve(controller: AtmReferenceController, **overrides: object):
    kwargs: dict[str, object] = {
        "strike_step": 5,
        "is_rth": False,
        "trading_date": TRADING_DATE,
        "trading_days_since_basis": None,
    }
    kwargs.update(overrides)
    return controller.resolve(**kwargs)


def qualify_basis(controller: AtmReferenceController, *, contract: str = "ESU6") -> None:
    for offset, basis in zip((0, 8, 16, 24, 32), (50, 51, 49, 50, 50), strict=True):
        result = resolve(
            controller,
            is_rth=True,
            trading_days_since_basis=0,
            spx=quote(7500, seconds=offset),
            es=quote(7500 + basis, seconds=offset + 1, contract=contract),
        )
        assert result.candidate is not None
        assert result.candidate.source == "SPX"


def test_rth_fresh_spx_is_authoritative() -> None:
    result = resolve(
        AtmReferenceController(),
        is_rth=True,
        spx=quote(7502),
        ibus500=quote(7510),
        spy=quote(752),
    )

    assert result.candidate is not None
    assert result.candidate.value == 7502
    assert result.candidate.rounded_strike == 7500
    assert result.candidate.source == "SPX"
    assert result.candidate.reason == "rth_fresh_spx_authoritative"


def test_off_hours_prefers_fresh_ibus500_cash_proxy() -> None:
    result = resolve(
        AtmReferenceController(),
        ibus500=quote(7512),
        spy=quote(750),
    )

    assert result.candidate is not None
    assert result.candidate.source == "IBUS500"
    assert result.candidate.value == 7512


def test_es_is_eligible_only_with_qualified_same_contract_basis() -> None:
    controller = AtmReferenceController()
    no_basis = resolve(
        controller,
        es=quote(7550, contract="ESU6"),
        trading_days_since_basis=0,
    )
    assert no_basis.candidate is None

    qualify_basis(controller)
    result = resolve(
        controller,
        es=quote(7560, seconds=60, contract="ESU6"),
        trading_days_since_basis=0,
    )

    assert result.candidate is not None
    assert result.candidate.source == "ES_basis_adj"
    assert result.candidate.value == pytest.approx(7510)
    assert result.candidate.basis_value == pytest.approx(50)
    assert result.candidate.basis_contract == "ESU6"


def test_basis_rejects_skewed_implausible_and_median_outlier_samples() -> None:
    tracker = EsSpxBasisTracker()

    tracker.observe(
        spx=quote(7500),
        es=quote(7550, seconds=6, contract="ESU6"),
        is_rth=True,
        trading_date=TRADING_DATE,
    )
    tracker.observe(
        spx=quote(7500, seconds=10),
        es=quote(7630, seconds=10, contract="ESU6"),
        is_rth=True,
        trading_date=TRADING_DATE,
    )
    assert tracker.state is None

    tracker.observe(
        spx=quote(7500, seconds=20),
        es=quote(7550, seconds=20, contract="ESU6"),
        is_rth=True,
        trading_date=TRADING_DATE,
    )
    tracker.observe(
        spx=quote(7500, seconds=30),
        es=quote(7570, seconds=30, contract="ESU6"),
        is_rth=True,
        trading_date=TRADING_DATE,
    )

    assert tracker.state is not None
    assert tracker.state.sample_count == 1
    assert tracker.state.samples[0].value == 50


def test_basis_needs_five_distinct_samples_spanning_thirty_seconds() -> None:
    tracker = EsSpxBasisTracker()
    for offset in (0, 5, 10, 15, 20):
        tracker.observe(
            spx=quote(7500, seconds=offset),
            es=quote(7550, seconds=offset, contract="ESU6"),
            is_rth=True,
            trading_date=TRADING_DATE,
        )
    assert tracker.state is not None
    assert tracker.state.median is None

    tracker.observe(
        spx=quote(7500, seconds=30),
        es=quote(7550, seconds=30, contract="ESU6"),
        is_rth=True,
        trading_date=TRADING_DATE,
    )
    assert tracker.state is not None
    assert tracker.state.sample_count == 6
    assert tracker.state.median == 50


def test_basis_expires_after_three_trading_days() -> None:
    controller = AtmReferenceController()
    qualify_basis(controller)

    still_valid = resolve(
        controller,
        es=quote(7560, seconds=60, contract="ESU6"),
        trading_days_since_basis=3,
    )
    expired = resolve(
        controller,
        es=quote(7560, seconds=61, contract="ESU6"),
        trading_days_since_basis=4,
    )

    assert still_valid.candidate is not None
    assert still_valid.candidate.source == "ES_basis_adj"
    assert expired.candidate is None


def test_es_contract_change_invalidates_basis() -> None:
    controller = AtmReferenceController()
    qualify_basis(controller, contract="ESU6")

    result = resolve(
        controller,
        es=quote(7560, seconds=60, contract="ESZ6"),
        trading_days_since_basis=0,
    )

    assert result.candidate is None
    assert controller.basis_tracker.state is None


def test_fresh_spy_times_ten_is_the_last_fresh_proxy() -> None:
    result = resolve(
        AtmReferenceController(),
        es=quote(7550, contract="ESU6"),
        spy=quote(750.4),
    )

    assert result.candidate is not None
    assert result.candidate.source == "SPY*10"
    assert result.candidate.value == 7504
    assert result.candidate.rounded_strike == 7505


def test_stale_spx_is_a_persisted_one_time_bootstrap(tmp_path) -> None:
    state_path = tmp_path / "atm-state.json"
    controller = AtmReferenceController(state_path)
    stale_spx = quote(7482.71, freshness="stale")

    first = resolve(controller, spx=stale_spx)
    retry_before_acceptance = resolve(controller, spx=stale_spx)

    assert first.candidate is not None
    assert first.candidate.source == "SPX_stale_bootstrap"
    assert retry_before_acceptance.candidate is not None

    controller.record_accepted(first.candidate, expiry="20260709")
    second = resolve(controller, spx=stale_spx)
    after_restart = resolve(AtmReferenceController(state_path), spx=stale_spx)

    assert second.candidate is None
    assert after_restart.candidate is None


@pytest.mark.parametrize("freshness", ["delayed", "frozen", "unknown", "close_only"])
def test_non_stale_spx_modes_cannot_use_stale_bootstrap(freshness: str) -> None:
    result = resolve(
        AtmReferenceController(),
        spx=quote(7482.71, freshness=freshness),
    )

    assert result.candidate is None


def test_accepted_atm_is_reused_only_for_expiry_rollover(tmp_path) -> None:
    state_path = tmp_path / "atm-state.json"
    controller = AtmReferenceController(state_path)
    initial = resolve(controller, ibus500=quote(7512))
    assert initial.candidate is not None
    controller.record_accepted(initial.candidate, expiry="20260709")

    restarted = AtmReferenceController(state_path)
    steady = resolve(restarted)
    rollover = resolve(restarted, expiry_rollover=True)

    assert steady.candidate is None
    assert rollover.candidate is not None
    assert rollover.candidate.source == "stable_atm"
    assert rollover.candidate.rounded_strike == 7510
    assert restarted.stable_atm is not None
    assert restarted.stable_atm.expiry == "20260709"


def test_controller_persists_basis_atomically_with_owner_only_mode(tmp_path) -> None:
    state_path = tmp_path / "atm-state.json"
    controller = AtmReferenceController(state_path)
    qualify_basis(controller)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    restarted = AtmReferenceController(state_path)

    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    assert payload["basis"]["sample_count"] == 5
    assert payload["basis"]["median"] == 50
    assert restarted.basis_tracker.state is not None
    assert restarted.basis_tracker.state.median == 50


def test_persisted_basis_must_be_supported_by_qualified_sample_evidence(tmp_path) -> None:
    state_path = tmp_path / "atm-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "basis": {
                    "es_contract": "ESU6",
                    "trading_date": "2026-07-09",
                    "samples": [],
                    "sample_count": 0,
                    "sample_window_start": None,
                    "median": 1000.0,
                    "observed_at": None,
                },
                "stable_atm": None,
                "stale_spx_bootstrap_used": False,
            }
        ),
        encoding="utf-8",
    )

    controller = AtmReferenceController(state_path)
    result = resolve(
        controller,
        es=quote(7550, contract="ESU6"),
        trading_days_since_basis=0,
    )

    assert controller.basis_tracker.state is None
    assert result.candidate is None


def test_malformed_persisted_state_fails_closed(tmp_path) -> None:
    state_path = tmp_path / "atm-state.json"
    state_path.write_text('{"schema_version": 1, "basis": {}}', encoding="utf-8")

    controller = AtmReferenceController(state_path)

    assert controller.basis_tracker.state is None
    assert controller.stable_atm is None
    assert controller.stale_spx_bootstrap_used is False
