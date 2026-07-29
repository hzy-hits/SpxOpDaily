from __future__ import annotations

import json
import os
import runpy
from contextlib import nullcontext
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spx_spark.config import NY_TZ


UTC = timezone.utc
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "warm_market_state_ma_history.py"


def _script() -> dict[str, Any]:
    return runpy.run_path(str(SCRIPT))


def _session_rows(
    trading_day: date,
    *,
    contract_identity: str = "ES:202609",
) -> list[dict[str, object]]:
    start = datetime.combine(trading_day, time(9, 30), tzinfo=NY_TZ)
    return [
        {
            "bar_start": (start + timedelta(minutes=5 * index)).astimezone(UTC).isoformat(),
            "bar_end": (start + timedelta(minutes=5 * (index + 1))).astimezone(UTC).isoformat(),
            "interval_seconds": 300,
            "open": 7400.0 + index / 10,
            "high": 7401.0 + index / 10,
            "low": 7399.0 + index / 10,
            "close": 7400.5 + index / 10,
            "quality": "ok",
            "gap_before": False,
            "segment": "rth",
            "trading_date_et": trading_day.isoformat(),
            "contract_identity": contract_identity,
            "contract_identity_ambiguous": False,
        }
        for index in range(78)
    ]


def test_identity_and_completed_session_are_deterministic() -> None:
    namespace = _script()

    assert namespace["_canonical_identity"]("20260918") == "ES:202609"
    trading_day, close = namespace["_latest_completed_session"](
        datetime(2026, 7, 25, 16, 0, tzinfo=UTC)
    )

    assert trading_day == date(2026, 7, 24)
    assert close == datetime(2026, 7, 24, 20, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="yyyymmdd"):
        namespace["_canonical_identity"]("202609")


def test_qualified_contract_requires_exact_es_future() -> None:
    namespace = _script()

    class FakeIB:
        @staticmethod
        def qualifyContracts(_contract: object) -> list[object]:
            return [
                SimpleNamespace(
                    conId=123,
                    secType="FUT",
                    symbol="ES",
                    exchange="CME",
                    lastTradeDateOrContractMonth="20260918",
                )
            ]

    contract = namespace["_qualified_es_contract"](
        FakeIB(),
        expiry="20260918",
    )
    assert contract.conId == 123

    class WrongIB:
        @staticmethod
        def qualifyContracts(_contract: object) -> list[object]:
            return [
                SimpleNamespace(
                    conId=123,
                    secType="FUT",
                    symbol="NQ",
                    exchange="CME",
                    lastTradeDateOrContractMonth="20260918",
                )
            ]

    with pytest.raises(RuntimeError, match="identity_mismatch"):
        namespace["_qualified_es_contract"](WrongIB(), expiry="20260918")


def test_historical_rows_are_exact_closed_rth_bars() -> None:
    namespace = _script()
    start = datetime.combine(date(2026, 7, 24), time(9, 30), tzinfo=NY_TZ)
    values = [
        SimpleNamespace(
            date=start + timedelta(minutes=5 * index),
            open=7400.0 + index,
            high=7401.0 + index,
            low=7399.0 + index,
            close=7400.5 + index,
        )
        for index in range(3)
    ]
    prior_cash_close = datetime.combine(
        date(2026, 7, 23),
        time(16, 0),
        tzinfo=NY_TZ,
    )
    values.append(
        SimpleNamespace(
            date=prior_cash_close,
            open=7400.0,
            high=7401.0,
            low=7399.0,
            close=7400.5,
        )
    )

    class FakeIB:
        @staticmethod
        def reqHistoricalData(
            _contract: object,
            **_kwargs: object,
        ) -> list[object]:
            return values

    rows = namespace["_request_exact_rth_bars"](
        FakeIB(),
        object(),
        end_at=datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
        duration="1 M",
        contract_identity="ES:202609",
    )

    assert len(rows) == 3
    assert rows[0]["bar_start"] == start.astimezone(UTC).isoformat()
    assert all(row["quality"] == "ok" for row in rows)
    assert all(row["segment"] == "rth" for row in rows)
    assert all(row["contract_identity"] == "ES:202609" for row in rows)
    assert not any(row["gap_before"] for row in rows)

    values.insert(
        0,
        SimpleNamespace(
            date=start - timedelta(minutes=5),
            open=7400.0,
            high=7401.0,
            low=7399.0,
            close=7400.5,
        ),
    )
    with pytest.raises(RuntimeError, match="outside_requested_rth"):
        namespace["_request_exact_rth_bars"](
            FakeIB(),
            object(),
            end_at=datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
            duration="1 M",
            contract_identity="ES:202609",
        )
    values.pop(0)
    values.append(
        SimpleNamespace(
            date=datetime.combine(
                date(2026, 7, 24),
                time(16, 0),
                tzinfo=NY_TZ,
            ),
            open=7400.0,
            high=7401.0,
            low=7399.0,
            close=7400.5,
        )
    )
    with pytest.raises(RuntimeError, match="after_requested_cutoff"):
        namespace["_request_exact_rth_bars"](
            FakeIB(),
            object(),
            end_at=datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
            duration="1 M",
            contract_identity="ES:202609",
        )


def test_overlap_audit_fails_closed_on_price_or_contract_drift() -> None:
    namespace = _script()
    seed = _session_rows(date(2026, 7, 24))
    live = [{**row, "close": float(row["close"]) + 0.25} for row in seed]
    state = {"closed_bars": live}
    cutoff = datetime(2026, 7, 24, 20, 0, tzinfo=UTC)

    audit = namespace["_audit_overlap"](
        state,
        seed,
        contract_identity="ES:202609",
        minimum=6,
        max_close_difference=1.0,
        cutoff=cutoff,
    )
    assert audit == {"count": 78, "max_close_difference": 0.25}

    with pytest.raises(RuntimeError, match="difference_too_large"):
        namespace["_audit_overlap"](
            state,
            seed,
            contract_identity="ES:202609",
            minimum=6,
            max_close_difference=0.1,
            cutoff=cutoff,
        )
    state["closed_bars"][-1]["contract_identity"] = "ES:202612"
    with pytest.raises(RuntimeError, match="identity_or_quality_unverified"):
        namespace["_audit_overlap"](
            state,
            seed,
            contract_identity="ES:202609",
            minimum=6,
            max_close_difference=1.0,
            cutoff=cutoff,
        )


def test_overlap_requires_complete_latest_live_session_and_hard_floor() -> None:
    namespace = _script()
    seed = _session_rows(date(2026, 7, 24))
    cutoff = datetime(2026, 7, 24, 20, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="minimum_too_low"):
        namespace["_audit_overlap"](
            {"closed_bars": seed},
            seed,
            contract_identity="ES:202609",
            minimum=5,
            max_close_difference=2.0,
            cutoff=cutoff,
        )
    for unsafe_limit in (float("nan"), float("inf"), 2.01, 100.0):
        with pytest.raises(RuntimeError, match="difference_limit_invalid"):
            namespace["_audit_overlap"](
                {"closed_bars": seed},
                seed,
                contract_identity="ES:202609",
                minimum=6,
                max_close_difference=unsafe_limit,
                cutoff=cutoff,
            )
    with pytest.raises(RuntimeError, match="bucket_count_incomplete"):
        namespace["_audit_overlap"](
            {"closed_bars": seed[:-1]},
            seed,
            contract_identity="ES:202609",
            minimum=6,
            max_close_difference=2.0,
            cutoff=cutoff,
        )
    duplicated = [*seed[:-1], dict(seed[-2])]
    with pytest.raises(RuntimeError, match="bucket_identity_incomplete"):
        namespace["_audit_overlap"](
            {"closed_bars": duplicated},
            seed,
            contract_identity="ES:202609",
            minimum=6,
            max_close_difference=2.0,
            cutoff=cutoff,
        )


def test_state_and_seed_cutoff_validation_fail_closed(tmp_path: Path) -> None:
    namespace = _script()
    state_path = tmp_path / "es_bars_5m.json"
    state_path.write_text("{broken", encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid_json"):
        namespace["_strict_state_snapshot"](state_path)

    state_path.write_text(
        json.dumps({"schema_version": "wrong", "interval_seconds": 300}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="schema_mismatch"):
        namespace["_strict_state_snapshot"](state_path)

    rows = _session_rows(date(2026, 7, 24))
    with pytest.raises(RuntimeError, match="do_not_reach_cutoff"):
        namespace["_assert_seed_cutoff"](
            rows[:-1],
            cutoff=datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
        )


def test_contract_eligibility_requires_non_conflicting_live_identity() -> None:
    namespace = _script()
    state = {
        "closed_bars": _session_rows(date(2026, 7, 24)),
        "contract_identity": None,
        "current_bar": {},
    }
    assert (
        namespace["_assert_state_contract_eligible"](
            state,
            contract_identity="ES:202609",
        )
        is True
    )
    state["contract_identity"] = "ES:202612"
    with pytest.raises(RuntimeError, match="live_state_contract_identity_mismatch"):
        namespace["_assert_state_contract_eligible"](
            state,
            contract_identity="ES:202609",
        )
    state["contract_identity"] = "ES:202609"
    state["current_bar"] = {"contract_identity_ambiguous": True}
    with pytest.raises(RuntimeError, match="current_bar_contract_identity_ambiguous"):
        namespace["_assert_state_contract_eligible"](
            state,
            contract_identity="ES:202609",
        )


@pytest.mark.parametrize(
    ("status", "returncode", "accepted"),
    [
        ("inactive", 3, True),
        ("active", 0, False),
        ("activating", 0, False),
        ("deactivating", 0, False),
        ("failed", 3, False),
        ("", 1, False),
    ],
)
def test_worker_must_be_exactly_inactive(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    returncode: int,
    accepted: bool,
) -> None:
    namespace = _script()
    monkeypatch.setattr(
        namespace["subprocess"],
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            stdout=f"{status}\n" if status else "",
            stderr="dbus error" if not status else "",
            returncode=returncode,
        ),
    )
    if accepted:
        namespace["_assert_worker_inactive"]("worker.service")
    else:
        with pytest.raises(RuntimeError, match="must_be_inactive"):
            namespace["_assert_worker_inactive"]("worker.service")


def test_stopped_sampler_guard_uses_fixed_unit_and_process_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _script()
    script_globals = namespace["_stopped_sampler_guard"].__wrapped__.__globals__
    lock_path = tmp_path / "hot-worker.lock"
    observed: list[str] = []
    monkeypatch.setitem(script_globals, "default_lock_path", lambda: lock_path)
    monkeypatch.setitem(
        script_globals,
        "_assert_worker_inactive",
        lambda unit: observed.append(unit),
    )

    with namespace["_stopped_sampler_guard"]():
        assert lock_path.exists()

    assert observed == [
        "spx-spark-es-bar-sampler.service",
        "spx-spark-es-bar-sampler.service",
    ]

    with namespace["ProcessLock"](lock_path):
        with pytest.raises(RuntimeError, match="process_lock_is_held"):
            with namespace["_stopped_sampler_guard"]():
                pass


def test_backup_is_unique_owner_only_and_exact(tmp_path: Path) -> None:
    namespace = _script()
    state_path = tmp_path / "es_bars_5m.json"
    payload = b'{"schema_version":"es_5m_bar_state.v1"}\n'
    sha = namespace["_sha256"](payload)

    backup = namespace["_write_backup"](state_path, payload, sha256=sha)
    second = namespace["_write_backup"](state_path, payload, sha256=sha)

    assert backup.read_bytes() == payload
    assert second.read_bytes() == payload
    assert backup != second
    assert os.stat(backup).st_mode & 0o777 == 0o600
    assert os.stat(second).st_mode & 0o777 == 0o600


def test_atomic_restore_preserves_exact_original_bytes(tmp_path: Path) -> None:
    namespace = _script()
    state_path = tmp_path / "es_bars_5m.json"
    original = b'{"schema_version":"es_5m_bar_state.v1","interval_seconds":300}\n'
    state_path.write_bytes(b"replacement")

    namespace["_atomic_write_bytes_secure"](state_path, original)

    assert state_path.read_bytes() == original
    assert os.stat(state_path).st_mode & 0o777 == 0o600


def test_apply_restores_exact_original_when_post_write_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _script()
    rows = [row for day in (20, 21, 22, 23, 24) for row in _session_rows(date(2026, 7, day))]
    state = {
        "schema_version": "es_5m_bar_state.v1",
        "interval_seconds": 300,
        "contract_identity": "ES:202609",
        "current_bar": {},
        "closed_bars": rows[-78:],
        "rth_ma_history": [],
        "diagnostics": {},
    }
    state_path = tmp_path / "es_bars_5m.json"
    original = (json.dumps(state, indent=1) + "\n").encode()
    state_path.write_bytes(original)
    args = SimpleNamespace(
        host="127.0.0.1",
        port=4002,
        client_id=299,
        es_expiry="20260918",
        duration="1 M",
        state_path=state_path,
        min_overlap_bars=6,
        max_overlap_close_difference=2.0,
        apply=True,
    )
    contract = SimpleNamespace(
        conId=123,
        secType="FUT",
        symbol="ES",
        exchange="CME",
        lastTradeDateOrContractMonth="20260918",
        localSymbol="ESU6",
    )

    class FakeIB:
        @staticmethod
        def connect(*_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def disconnect() -> None:
            return None

        @staticmethod
        def qualifyContracts(_contract: object) -> list[object]:
            return [contract]

    ready_calls = 0

    def fail_second_ready(
        _state: dict[str, object],
        *,
        contract_identity: str,
        cutoff: datetime,
    ) -> dict[str, object]:
        nonlocal ready_calls
        assert contract_identity == "ES:202609"
        assert cutoff == datetime(2026, 7, 24, 20, 0, tzinfo=UTC)
        ready_calls += 1
        if ready_calls == 2:
            raise RuntimeError("injected_post_write_failure")
        return {
            "status": "ready",
            "regime_state": "TREND_ALIGNED",
            "sma50": 7400.0,
            "sma200": 7390.0,
            "atr_5m": 5.0,
        }

    script_globals = namespace["main"].__globals__
    monkeypatch.setitem(script_globals, "parse_args", lambda: args)
    monkeypatch.setitem(script_globals, "IB", FakeIB)
    monkeypatch.setitem(
        script_globals,
        "_latest_completed_session",
        lambda _now: (
            date(2026, 7, 24),
            datetime(2026, 7, 24, 20, 0, tzinfo=UTC),
        ),
    )
    monkeypatch.setitem(
        script_globals,
        "_request_exact_rth_bars",
        lambda *_args, **_kwargs: rows,
    )
    monkeypatch.setitem(
        script_globals,
        "_stopped_sampler_guard",
        lambda: nullcontext(),
    )
    monkeypatch.setitem(script_globals, "_assert_ready", fail_second_ready)

    with pytest.raises(RuntimeError, match="write_failed_original_restored"):
        namespace["main"]()

    assert ready_calls == 2
    assert state_path.read_bytes() == original
    backups = list((tmp_path / "state-backups").glob("*.bak"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == original
