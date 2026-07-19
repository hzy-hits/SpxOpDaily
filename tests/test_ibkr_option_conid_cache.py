from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.stream.contracts import option_contracts_from_specs
from spx_spark.ibkr.stream.models import OptionSubscriptionPlan
from spx_spark.ibkr.stream.option_conid_cache import (
    CACHE_KIND,
    CACHE_SCHEMA_VERSION,
    SpxwConIdCache,
)
from spx_spark.ibkr.verifier import IbkrError, VerifyRow
from spx_spark.sampling import OptionContractSpec
from stream_test_helpers import patch_stream


EXPIRY = "20260720"
LABEL = f"option:SPXW:{EXPIRY}:7500:C"


def _contract(
    *,
    expiry: str = EXPIRY,
    strike: int = 7500,
    right: str = "C",
    con_id: int = 900_001,
    trading_class: str = "SPXW",
) -> SimpleNamespace:
    return SimpleNamespace(
        conId=con_id,
        secType="OPT",
        symbol="SPX",
        lastTradeDateOrContractMonth=expiry,
        strike=float(strike),
        right=right,
        exchange="SMART",
        currency="USD",
        multiplier="100",
        tradingClass=trading_class,
    )


def _calendar(expiry: str = EXPIRY) -> SimpleNamespace:
    return SimpleNamespace(
        research_expiry=lambda _now: date(
            int(expiry[:4]),
            int(expiry[4:6]),
            int(expiry[6:]),
        )
    )


def _collector(cache_path, ib, *, expiry: str = EXPIRY) -> StreamCollector:
    collector = object.__new__(StreamCollector)
    collector.ib = ib
    collector.market_calendar = _calendar(expiry)
    collector.qualified_option_contracts = {}
    collector.option_conid_cache = SpxwConIdCache(cache_path)
    collector._option_conid_cache_expiries = frozenset()
    collector.option_definition_resolution_sources = {}
    return collector


def test_current_research_expiry_cache_round_trips_strict_identity(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    cache = SpxwConIdCache(path)

    assert cache.remember([(LABEL, "option", _contract())], expiry=EXPIRY) == 1

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == CACHE_SCHEMA_VERSION
    assert payload["cache_kind"] == CACHE_KIND
    assert payload["active_expiries"] == [EXPIRY]
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(path.parent.glob(f".{path.name}.*.tmp"))

    restarted = SpxwConIdCache(path)
    assert restarted.prepare(EXPIRY) == 1
    assert restarted.cached_con_id(LABEL, _contract(con_id=0), expiry=EXPIRY) == 900_001
    assert (
        restarted.cached_con_id(
            LABEL,
            _contract(con_id=0, trading_class="SPX"),
            expiry=EXPIRY,
        )
        is None
    )


def test_missing_cache_prepare_is_read_only_until_first_remember(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    cache = SpxwConIdCache(path)

    assert cache.prepare((EXPIRY, "20260721")) == 0
    assert cache.active_expiries == frozenset({EXPIRY, "20260721"})
    assert not path.exists()
    assert not path.with_name(f"{path.name}.lock").exists()

    assert cache.remember([(LABEL, "option", _contract())], expiry=EXPIRY) == 1
    assert path.exists()
    assert path.with_name(f"{path.name}.lock").exists()


def test_stale_expiry_and_corrupt_files_are_atomically_pruned(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    old = SpxwConIdCache(path)
    old.remember([(LABEL, "option", _contract())], expiry=EXPIRY)

    rolled = SpxwConIdCache(path)
    assert rolled.prepare("20260721") == 0
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["active_expiries"] == ["20260721"]
    assert payload["contracts"] == []

    path.write_text("{broken", encoding="utf-8")
    corrupt = SpxwConIdCache(path)
    assert corrupt.prepare("20260721") == 0
    repaired = json.loads(path.read_text(encoding="utf-8"))
    assert repaired["active_expiries"] == ["20260721"]
    assert repaired["contracts"] == []

    repaired["active_expiries"] = [{}]
    path.write_text(json.dumps(repaired), encoding="utf-8")
    malformed = SpxwConIdCache(path)
    assert malformed.prepare("20260721") == 0
    assert json.loads(path.read_text(encoding="utf-8"))["active_expiries"] == ["20260721"]


def test_cache_rejects_non_spxw_wrong_expiry_and_unqualified_contracts(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    cache = SpxwConIdCache(path)

    assert (
        cache.remember(
            [(LABEL, "option", _contract(trading_class="SPX"))],
            expiry=EXPIRY,
        )
        == 0
    )
    assert cache.remember([(LABEL, "option", _contract(con_id=0))], expiry=EXPIRY) == 0
    assert (
        cache.remember(
            [(LABEL, "option", _contract(expiry="20260721"))],
            expiry=EXPIRY,
        )
        == 0
    )
    assert not path.exists()


def test_resolve_reuses_persisted_conid_after_collector_restart(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    spec = OptionContractSpec(expiry=EXPIRY, strike=7500, right="C", lane="hot")

    class QualifyingIB:
        RequestTimeout = 30.0

        def __init__(self) -> None:
            self.calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            contracts[0].conId = 900_001
            return list(contracts)

    first_ib = QualifyingIB()
    first = _collector(path, first_ib)
    first_result = first._resolve_option_definitions(option_contracts_from_specs((spec,)))
    assert first_ib.calls == 1
    assert first_result[0][2].conId == 900_001
    assert first.option_definition_resolution_source(LABEL) == "ibkr_qualification"

    second_ib = QualifyingIB()
    restarted = _collector(path, second_ib)
    second_result = restarted._resolve_option_definitions(option_contracts_from_specs((spec,)))

    assert second_ib.calls == 0
    assert second_result[0][2].conId == 900_001
    assert restarted.option_definition_resolution_source(LABEL) == "durable_cache"


def test_mixed_current_next_plan_persists_and_restarts_without_qualification(
    tmp_path,
) -> None:
    path = tmp_path / "state" / "conids.json"
    next_expiry = "20260721"
    current_label = f"option:SPXW:{EXPIRY}:7500:C"
    next_label = f"option:SPXW:{next_expiry}:7525:P"
    specs = (
        OptionContractSpec(expiry=EXPIRY, strike=7500, right="C", lane="hot"),
        OptionContractSpec(expiry=next_expiry, strike=7525, right="P", lane="hot"),
    )
    calendar = SimpleNamespace(
        research_expiry=lambda _now: date(2026, 7, 20),
        option_collection_expiries=lambda _now: (
            date(2026, 7, 20),
            date(2026, 7, 21),
        ),
    )

    class QualifyingIB:
        RequestTimeout = 30.0

        def __init__(self) -> None:
            self.calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            for offset, contract in enumerate(contracts):
                contract.conId = 910_000 + offset
            return list(contracts)

    first_ib = QualifyingIB()
    first = _collector(path, first_ib)
    first.market_calendar = calendar
    first._resolve_option_definitions(option_contracts_from_specs(specs))

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert first_ib.calls == 1
    assert payload["active_expiries"] == [EXPIRY, next_expiry]
    assert {entry["label"] for entry in payload["contracts"]} == {
        current_label,
        next_label,
    }
    assert first.option_definition_resolution_source(current_label) == "ibkr_qualification"
    assert first.option_definition_resolution_source(next_label) == "ibkr_qualification"

    second_ib = QualifyingIB()
    restarted = _collector(path, second_ib)
    restarted.market_calendar = calendar
    resolved = restarted._resolve_option_definitions(option_contracts_from_specs(specs))

    assert second_ib.calls == 0
    assert [definition[2].conId for definition in resolved] == [910_000, 910_001]
    assert restarted.option_definition_resolution_source(current_label) == "durable_cache"
    assert restarted.option_definition_resolution_source(next_label) == "durable_cache"

    restarted._resolve_option_definitions(option_contracts_from_specs(specs))
    assert second_ib.calls == 0
    assert restarted.option_definition_resolution_source(current_label) == "memory_cache"
    assert restarted.option_definition_resolution_source(next_label) == "memory_cache"


def test_real_mixed_expiry_option_plan_restarts_with_zero_qualification(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "state" / "conids.json"
    next_expiry = "20260721"
    specs = (
        OptionContractSpec(expiry=EXPIRY, strike=7500, right="C", lane="hot"),
        OptionContractSpec(expiry=next_expiry, strike=7525, right="P", lane="hot"),
    )
    plan = OptionSubscriptionPlan(
        atm_strike=7500,
        expiry=EXPIRY,
        hot=specs,
        rotations=(),
    )
    calendar = SimpleNamespace(
        research_expiry=lambda _now: date(2026, 7, 20),
        option_collection_expiries=lambda _now: (
            date(2026, 7, 20),
            date(2026, 7, 21),
        ),
    )

    class QualifyingIB:
        RequestTimeout = 30.0

        def __init__(self) -> None:
            self.calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            for offset, contract in enumerate(contracts):
                contract.conId = 920_000 + offset
            return list(contracts)

        def isConnected(self) -> bool:
            return True

        def sleep(self, _seconds: float) -> None:
            return None

    request_id = 30_000

    def subscribe(_ib, definitions, *, qualify=False):
        nonlocal request_id
        assert qualify is False
        subscriptions = {}
        for label, kind, contract in definitions:
            request_id += 1
            subscriptions[label] = (
                SimpleNamespace(contract=contract),
                VerifyRow(
                    label=label,
                    kind=kind,
                    symbol="SPX",
                    subscribed=True,
                    request_id=request_id,
                ),
            )
        return subscriptions

    patch_stream(monkeypatch, "qualify_and_subscribe", subscribe)
    patch_stream(monkeypatch, "cancel_subscriptions", lambda *_args: True)

    def plan_collector(ib) -> StreamCollector:
        collector = _collector(path, ib)
        collector.market_calendar = calendar
        collector.stream_settings = SimpleNamespace(max_option_lines=84)
        collector.hot_subs = {}
        collector.rotation_subs = {}
        collector.pinned_subs = {}
        collector.option_plan = None
        collector.rotation_index = 0
        collector.subscription_rejection_sequence = 0
        collector.subscription_rejection_log = []
        collector.subscription_rows_by_req_id = {}
        collector.subscription_lane_by_req_id = {}
        collector.subscription_health_failed = False
        collector.tws_connectivity_lost = False
        collector.subscriptions_lost = False
        collector.tws_connectivity_loss_sequence = 0
        collector.capacity_tracker = None
        return collector

    first_ib = QualifyingIB()
    first = plan_collector(first_ib)
    assert first.reconcile_option_plan(plan) is True
    assert first_ib.calls == 1
    assert len(json.loads(path.read_text(encoding="utf-8"))["contracts"]) == 2

    second_ib = QualifyingIB()
    restarted = plan_collector(second_ib)
    assert restarted.reconcile_option_plan(plan) is True
    assert second_ib.calls == 0
    assert len(restarted.hot_subs) == 2


def test_rollover_prunes_only_expired_bucket_and_keeps_overlap(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    next_expiry = "20260721"
    following_expiry = "20260722"
    next_label = f"option:SPXW:{next_expiry}:7525:P"
    cache = SpxwConIdCache(path)
    cache.prepare((EXPIRY, next_expiry))
    cache.remember([(LABEL, "option", _contract())], expiry=EXPIRY)
    cache.remember(
        [
            (
                next_label,
                "option",
                _contract(
                    expiry=next_expiry,
                    strike=7525,
                    right="P",
                    con_id=900_002,
                ),
            )
        ],
        expiry=next_expiry,
    )

    rolled = SpxwConIdCache(path)
    assert rolled.prepare((next_expiry, following_expiry)) == 1

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["active_expiries"] == [next_expiry, following_expiry]
    assert [entry["label"] for entry in payload["contracts"]] == [next_label]
    assert rolled.cached_con_id(LABEL, _contract(con_id=0), expiry=EXPIRY) is None
    assert (
        rolled.cached_con_id(
            next_label,
            _contract(expiry=next_expiry, strike=7525, right="P", con_id=0),
            expiry=next_expiry,
        )
        == 900_002
    )


def test_prefetch_collection_expiry_is_persisted_before_research_rollover(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    prefetch_expiry = "20260721"
    spec = OptionContractSpec(expiry=prefetch_expiry, strike=7500, right="C", lane="hot")
    calendar = SimpleNamespace(
        research_expiry=lambda _now: date(2026, 7, 20),
        option_collection_expiry=lambda _now: date(2026, 7, 21),
    )

    class QualifyingIB:
        RequestTimeout = 30.0

        def __init__(self) -> None:
            self.calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            contracts[0].conId = 900_003
            return list(contracts)

    first_ib = QualifyingIB()
    first = _collector(path, first_ib)
    first.market_calendar = calendar
    first._resolve_option_definitions(option_contracts_from_specs((spec,)))
    assert first_ib.calls == 1
    assert json.loads(path.read_text(encoding="utf-8"))["active_expiries"] == [
        prefetch_expiry
    ]

    second_ib = QualifyingIB()
    restarted = _collector(path, second_ib)
    restarted.market_calendar = calendar
    # open_session prepares the collection expiry before the first plan/resolve.
    restarted._prepare_option_definition_cache()
    assert json.loads(path.read_text(encoding="utf-8"))["active_expiries"] == [
        prefetch_expiry
    ]
    resolved = restarted._resolve_option_definitions(option_contracts_from_specs((spec,)))
    assert second_ib.calls == 0
    assert resolved[0][2].conId == 900_003


def test_corrupt_cache_falls_back_to_live_qualification(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    path.parent.mkdir(parents=True)
    path.write_text("not-json", encoding="utf-8")
    spec = OptionContractSpec(expiry=EXPIRY, strike=7500, right="C", lane="hot")

    class QualifyingIB:
        RequestTimeout = 30.0
        calls = 0

        def qualifyContracts(self, *contracts):
            self.calls += 1
            contracts[0].conId = 900_002
            return list(contracts)

    ib = QualifyingIB()
    collector = _collector(path, ib)
    result = collector._resolve_option_definitions(option_contracts_from_specs((spec,)))

    assert ib.calls == 1
    assert result[0][2].conId == 900_002


def test_ibkr_error_200_evicts_memory_and_durable_conid(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    contract = _contract()
    next_expiry = "20260721"
    next_label = f"option:SPXW:{next_expiry}:7525:P"
    next_contract = _contract(
        expiry=next_expiry,
        strike=7525,
        right="P",
        con_id=900_002,
    )
    cache = SpxwConIdCache(path)
    cache.prepare((EXPIRY, next_expiry))
    cache.remember([(LABEL, "option", contract)], expiry=EXPIRY)
    cache.remember([(next_label, "option", next_contract)], expiry=next_expiry)
    collector = _collector(path, SimpleNamespace())
    collector.market_calendar = SimpleNamespace(
        research_expiry=lambda _now: date(2026, 7, 20),
        option_collection_expiries=lambda _now: (
            date(2026, 7, 20),
            date(2026, 7, 21),
        ),
    )
    collector.qualified_option_contracts[LABEL] = (LABEL, "option", contract)
    collector.qualified_option_contracts[next_label] = (
        next_label,
        "option",
        next_contract,
    )
    collector.errors = []
    collector.capacity_tracker = None
    collector.subscription_rejection_sequence = 0
    collector.subscription_rejection_log = []
    collector.subscription_rows_by_req_id = {
        17: VerifyRow(
            label=LABEL,
            kind="option",
            symbol="SPX",
            subscribed=True,
            request_id=17,
        )
    }
    collector.subscription_lane_by_req_id = {17: "pinned"}
    collector.subscription_health_failed = False
    collector.tws_connectivity_lost = False
    collector.tws_connectivity_loss_sequence = 0
    collector.farm_health = SimpleNamespace(observe=lambda *_args: None)

    collector._on_error(17, 200, "No security definition has been found", contract)

    assert LABEL not in collector.qualified_option_contracts
    restarted = SpxwConIdCache(path)
    assert restarted.prepare((EXPIRY, next_expiry)) == 1
    assert restarted.cached_con_id(LABEL, _contract(con_id=0), expiry=EXPIRY) is None
    assert (
        restarted.cached_con_id(
            next_label,
            _contract(expiry=next_expiry, strike=7525, right="P", con_id=0),
            expiry=next_expiry,
        )
        == 900_002
    )


def test_setup_time_error_200_evicts_before_rows_are_registered(tmp_path) -> None:
    path = tmp_path / "state" / "conids.json"
    contract = _contract()
    cache = SpxwConIdCache(path)
    cache.remember([(LABEL, "option", contract)], expiry=EXPIRY)
    collector = _collector(path, SimpleNamespace())
    collector.qualified_option_contracts[LABEL] = (LABEL, "option", contract)
    row = VerifyRow(
        label=LABEL,
        kind="option",
        symbol="SPX",
        subscribed=True,
        request_id=22,
    )
    collector.subscription_rejection_log = [
        (
            1,
            IbkrError(
                req_id=22,
                error_code=200,
                message="No security definition has been found",
                contract=LABEL,
                ts="2026-07-20T00:00:00+00:00",
            ),
        )
    ]

    assert (
        collector._apply_subscription_rejections(
            {LABEL: (SimpleNamespace(contract=contract), row)},
            rejection_sequence=0,
        )
        is True
    )
    assert LABEL not in collector.qualified_option_contracts
    assert SpxwConIdCache(path).prepare(EXPIRY) == 0
