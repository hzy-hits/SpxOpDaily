from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import spx_spark.surface_dashboard as surface_dashboard
from spx_spark.config import StorageSettings
from spx_spark.features.exposure_surface import build_exposure_surface
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState, LatestStateStore
from spx_spark.surface_dashboard import (
    DASHBOARD_KIND,
    DASHBOARD_SCHEMA_VERSION,
    build_dashboard_snapshot,
    default_output_path,
    parse_args,
    resolve_output_path,
    run_loop,
    run_once,
)


NOW = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)


def storage_settings(tmp_path: Path) -> StorageSettings:
    data_root = tmp_path / "data"
    return StorageSettings(
        data_root=str(data_root),
        latest_state_path=str(data_root / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=120.0,
        slow_index_labels=frozenset({"index:SKEW"}),
        delayed_stale_after_seconds=60.0,
        rotation_stale_after_seconds=45.0,
    )


def index_quote(*, observed_at: datetime = NOW) -> Quote:
    return Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=observed_at,
        quality=MarketDataQuality.LIVE,
        bid=6299.0,
        ask=6301.0,
        last=6300.0,
        quote_time=observed_at,
        last_update_at=observed_at,
    )


def option_quote(
    expiry: str,
    strike: float,
    right: str,
    *,
    observed_at: datetime = NOW,
) -> Quote:
    offset = (strike - 6300.0) / 10.0
    price = 18.0 + abs(offset)
    if right == "C":
        price -= offset
    else:
        price += offset
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.SCHWAB,
        received_at=observed_at,
        quality=MarketDataQuality.LIVE,
        bid=price - 0.25,
        ask=price + 0.25,
        quote_time=observed_at,
        last_update_at=observed_at,
        greeks=OptionGreeks(implied_vol=0.20 + abs(offset) * 0.005),
        open_interest=100.0 + abs(offset) * 10.0,
        volume=20.0 + abs(offset),
    )


def option_chain(expiry: str, *, observed_at: datetime = NOW) -> list[Quote]:
    return [
        option_quote(expiry, strike, right, observed_at=observed_at)
        for strike in (6295.0, 6305.0)
        for right in ("C", "P")
    ]


def make_state(*quotes: Quote, now: datetime = NOW) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def research_expiries(now: datetime = NOW) -> tuple[str, str]:
    return tuple(
        expiry.strftime("%Y%m%d")
        for expiry in DEFAULT_MARKET_CALENDAR.research_expiries(now)
    )


def test_ready_snapshot_contains_only_front_and_next_surfaces(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    ignored = "20260722"
    state = make_state(
        index_quote(),
        *option_chain(front),
        *option_chain(next_expiry),
        *option_chain(ignored),
    )
    built_expiries: list[str] = []

    def counting_builder(contracts: list[object], **kwargs: object) -> object:
        rows = list(contracts)
        built_expiries.append(str(rows[0].expiry))
        return build_exposure_surface(rows, **kwargs)

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
        surface_builder=counting_builder,
    )

    assert payload["schema_version"] == DASHBOARD_SCHEMA_VERSION
    assert payload["kind"] == DASHBOARD_KIND
    assert payload["surface_version"] == "spxw_exposure_surface.v1"
    assert payload["status"] == "ready"
    assert payload["automatic_ordering"] is False
    assert payload["valid_until"] == (NOW + timedelta(seconds=10)).isoformat()
    assert payload["underlier"]["source"] == "index:SPX"
    assert payload["quality"] == {
        "status": "ready",
        "reasons": [],
        "requested_expiry_count": 2,
        "published_expiry_count": 2,
        "refresh_interval_seconds": 5.0,
        "lease_seconds": 10.0,
    }

    assert [(item["role"], item["expiry"]) for item in payload["expiries"]] == [
        ("front", front),
        ("next", next_expiry),
    ]
    assert built_expiries == [front, next_expiry]
    for expiry in payload["expiries"]:
        assert expiry["contract_count"] == 4
        assert expiry["call_count"] == 2
        assert expiry["put_count"] == 2
        assert expiry["providers"] == ["schwab"]
        assert [row["strike"] for row in expiry["strike_ladder"]] == [6295.0, 6305.0]
        assert all(
            row["call"] is not None and row["put"] is not None
            for row in expiry["strike_ladder"]
        )
        assert expiry["strike_ladder"] == expiry["surface"]["strike_ladder"]
        selected_metric = expiry["strike_ladder"][0]["weightings"]["oi_weighted"][
            "metrics"
        ]["gross_gamma"]
        assert isinstance(selected_metric, float)
        assert math.isfinite(selected_metric)
        assert expiry["quality"] == "ready"
        assert expiry["surface"]["quality"] == "ok"
        spot_count = len(expiry["surface"]["spot_grid"])
        metrics = expiry["surface"]["time_slices"][0]["weightings"]["oi_weighted"][
            "metrics"
        ]
        assert set(metrics) == {"signed_gamma", "gross_gamma", "charm", "vanna"}
        assert all(len(values) == spot_count for values in metrics.values())


def test_chain_implied_underlier_requires_fresh_coeval_pairs(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    state = make_state(*option_chain(front), *option_chain(next_expiry))

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
    )

    assert payload["status"] == "ready"
    assert payload["underlier"]["source"] == "chain_implied"
    assert payload["underlier"]["quality"] == "derived_fresh_pairs"
    assert payload["underlier"]["source_at"] is None


def test_chain_implied_underlier_rejects_leg_skew_over_five_seconds(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    skewed_front = [
        replace(
            quote,
            received_at=NOW - timedelta(seconds=6),
            quote_time=NOW - timedelta(seconds=6),
            last_update_at=NOW - timedelta(seconds=6),
        )
        if quote.instrument.right.value == "P"
        else quote
        for quote in option_chain(front)
    ]
    state = make_state(*skewed_front, *option_chain(next_expiry))

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
    )

    assert payload["status"] == "unavailable"
    assert payload["quality"]["reasons"] == ["underlier_unavailable"]
    assert payload["expiries"] == []


def test_stale_inputs_fail_closed_without_a_surface(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    stale_at = NOW - timedelta(minutes=10)
    state = make_state(
        index_quote(observed_at=stale_at),
        *option_chain(front, observed_at=stale_at),
        *option_chain(next_expiry, observed_at=stale_at),
    )

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
    )

    assert payload["status"] == "unavailable"
    assert payload["underlier"]["price"] is None
    assert payload["quality"]["reasons"] == ["underlier_unavailable"]
    assert payload["quality"]["published_expiry_count"] == 0
    assert payload["expiries"] == []


def test_partial_expiry_is_degraded_and_does_not_publish_an_empty_surface(
    tmp_path: Path,
) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    state = make_state(index_quote(), *option_chain(front))

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
    )

    assert payload["status"] == "degraded"
    assert [item["expiry"] for item in payload["expiries"]] == [front]
    assert payload["quality"]["reasons"] == ["next_fresh_iv_contracts_unavailable"]
    assert payload["quality"]["published_expiry_count"] == 1
    assert next_expiry not in json.dumps(payload["expiries"])


def test_bad_chain_math_publishes_unavailable_instead_of_crashing(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    state = make_state(index_quote(), *option_chain(front), *option_chain(next_expiry))

    def broken_builder(contracts: list[object], **kwargs: object) -> object:  # noqa: ARG001
        raise ZeroDivisionError("bad vendor input")

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
        surface_builder=broken_builder,
    )

    assert payload["status"] == "unavailable"
    assert payload["expiries"] == []
    assert payload["quality"]["reasons"] == [
        "front_surface_build_error:ZeroDivisionError",
        "next_surface_build_error:ZeroDivisionError",
    ]


def test_degraded_kernel_surfaces_remain_visible_with_root_reasons(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    state = make_state(index_quote(), *option_chain(front), *option_chain(next_expiry))

    class DegradedSurface:
        def to_dict(self) -> dict[str, object]:
            return {
                "quality": "degraded",
                "warnings": ["low_contract_coverage"],
                "strike_ladder": [],
            }

    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=NOW,
        surface_builder=lambda *args, **kwargs: DegradedSurface(),
    )

    assert payload["status"] == "degraded"
    assert payload["quality"]["reasons"] == [
        "front_surface_degraded",
        "next_surface_degraded",
    ]
    assert [item["quality"] for item in payload["expiries"]] == [
        "degraded",
        "degraded",
    ]


def test_run_once_atomically_writes_custom_public_output(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    front, next_expiry = research_expiries()
    state = make_state(index_quote(), *option_chain(front), *option_chain(next_expiry))
    LatestStateStore(settings).write(state)
    output_path = tmp_path / "published" / "snapshot.json"

    payload = run_once(
        storage_settings=settings,
        now=NOW,
        output_path=output_path,
    )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    assert output_path.stat().st_mode & 0o777 == 0o600
    assert list(output_path.parent.glob(".snapshot.json.*.tmp")) == []


def test_output_path_contract_supports_default_and_cwd_relative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = storage_settings(tmp_path)
    monkeypatch.chdir(tmp_path)

    assert default_output_path(settings) == Path(settings.data_root) / "latest" / (
        "spxw_surface_dashboard.json"
    )
    assert resolve_output_path("public/snapshot.json", settings) == (
        tmp_path / "public" / "snapshot.json"
    )
    args = parse_args(
        ["--once", "--json", "--interval-seconds", "2.5", "--output-path", "feed.json"]
    )
    assert args.once is True
    assert args.json is True
    assert args.interval_seconds == 2.5
    assert args.output_path == Path("feed.json")


def test_loop_uses_a_non_overlapping_start_anchored_cadence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = storage_settings(tmp_path)
    monotonic_values = iter((10.0, 10.2, 15.0, 15.1))
    waits: list[float] = []
    events: list[dict[str, object]] = []

    class FakeStopEvent:
        def is_set(self) -> bool:
            return False

        def set(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> bool:
            waits.append(float(timeout or 0.0))
            return False

    monkeypatch.setattr(
        surface_dashboard,
        "run_once",
        lambda **kwargs: {
            "status": "ready",
            "as_of": NOW.isoformat(),
            "valid_until": (NOW + timedelta(seconds=10)).isoformat(),
            "quality": {"published_expiry_count": 2},
        },
    )

    exit_code = run_loop(
        storage_settings=settings,
        interval_seconds=5.0,
        output_path=tmp_path / "snapshot.json",
        stop_event=FakeStopEvent(),
        max_cycles=2,
        monotonic=lambda: next(monotonic_values),
        utcnow=lambda: NOW,
        emit=events.append,
    )

    assert exit_code == 0
    assert waits == pytest.approx([4.8])
    assert [event["cycle"] for event in events] == [1, 2]
    assert [event["duration_ms"] for event in events] == [
        pytest.approx(200.0),
        pytest.approx(100.0),
    ]


def test_interval_must_be_positive_and_finite(tmp_path: Path) -> None:
    settings = storage_settings(tmp_path)
    state = make_state()

    with pytest.raises(ValueError, match="positive and finite"):
        build_dashboard_snapshot(
            state,
            storage_settings=settings,
            now=NOW,
            interval_seconds=0,
        )
