#!/usr/bin/env python3
"""Warm production ES MA history from one qualified IBKR futures contract.

The command is read-only unless ``--apply`` is present. Applying is restricted
to a stopped canonical ES bar sampler and performs a locked, compare-and-swap,
backed-up atomic state migration. It never writes historical bars into the hot
``closed_bars`` collection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from ib_async import IB, Future

from spx_spark.application.market_features.es_bar_state import (
    INTERVAL_SECONDS,
    MAX_RTH_MA_BARS,
    SCHEMA_VERSION,
    completed_es_bars,
    seed_rth_ma_history,
)
from spx_spark.application.market_features.moving_average_context import (
    moving_average_diagnostics,
)
from spx_spark.application.runtime.es_bar_sampler import (
    ProcessLock,
    ProcessLockUnavailable,
    default_lock_path,
)
from spx_spark.config import NY_TZ, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
)


UTC = timezone.utc
DEFAULT_SERVICE_UNIT = "spx-core.service"
MIN_APPLY_OVERLAP_BARS = 6
MAX_APPLY_OVERLAP_CLOSE_DIFFERENCE = 2.0
FUTURES_RTH_POST_CASH_CLOSE = time(17, 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch exact-contract IBKR RTH 5m ES bars and validate a bounded "
            "production MA200 warm-start. State is unchanged without --apply."
        )
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--client-id", type=int, default=299)
    parser.add_argument("--es-expiry", default="20260918")
    parser.add_argument("--duration", default="1 M")
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--min-overlap-bars", type=int, default=6)
    parser.add_argument("--max-overlap-close-difference", type=float, default=2.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _canonical_identity(expiry: str) -> str:
    if len(expiry) != 8 or not expiry.isdigit():
        raise ValueError("es_expiry_must_be_yyyymmdd")
    return f"ES:{expiry[:6]}"


def _latest_completed_session(now: datetime) -> tuple[date, datetime]:
    current = now.astimezone(NY_TZ)
    candidate = current.date()
    while True:
        session = DEFAULT_MARKET_CALENDAR.session(candidate)
        if session is not None and session.close_at <= current:
            return candidate, session.close_at.astimezone(UTC)
        candidate = DEFAULT_MARKET_CALENDAR.previous_trading_day(candidate)


def _qualified_es_contract(
    ib: IB,
    *,
    expiry: str,
) -> Any:
    contracts = ib.qualifyContracts(Future("ES", expiry, "CME"))
    if len(contracts) != 1:
        raise RuntimeError("ibkr_exact_es_contract_not_uniquely_qualified")
    contract = contracts[0]
    if (
        int(getattr(contract, "conId", 0) or 0) <= 0
        or str(getattr(contract, "secType", "")) != "FUT"
        or str(getattr(contract, "symbol", "")) != "ES"
        or str(getattr(contract, "exchange", "")) != "CME"
        or not str(getattr(contract, "lastTradeDateOrContractMonth", "")).startswith(expiry)
    ):
        raise RuntimeError("ibkr_qualified_contract_identity_mismatch")
    return contract


def _request_exact_rth_bars(
    ib: IB,
    contract: Any,
    *,
    end_at: datetime,
    duration: str,
    contract_identity: str,
) -> list[dict[str, object]]:
    values = ib.reqHistoricalData(
        contract,
        endDateTime=end_at.strftime("%Y%m%d-%H:%M:%S"),
        durationStr=duration,
        barSizeSetting="5 mins",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=2,
        keepUpToDate=False,
        timeout=45,
    )
    rows: list[dict[str, object]] = []
    previous_by_day: dict[str, datetime] = {}
    for item in values:
        start = item.date
        if not isinstance(start, datetime) or start.tzinfo is None:
            raise RuntimeError("ibkr_historical_bar_timestamp_unqualified")
        start = start.astimezone(UTC)
        end = start + timedelta(minutes=5)
        if end > end_at:
            raise RuntimeError("ibkr_historical_bar_after_requested_cutoff")
        local = start.astimezone(NY_TZ)
        session = DEFAULT_MARKET_CALENDAR.session(local.date())
        if session is None:
            raise RuntimeError("ibkr_historical_bar_outside_requested_rth")
        if not session.open_at <= local < session.close_at:
            # IBKR's futures useRTH window continues after the SPX cash close
            # (including equity early-close days). Exclude only that known CME
            # tail from a prior session; a cutoff-day tail already failed the
            # end-at check above. Every other off-window row fails closed.
            if not (session.close_at <= local and local.time() < FUTURES_RTH_POST_CASH_CLOSE):
                raise RuntimeError("ibkr_historical_bar_outside_requested_rth")
            continue
        prices = {key: _finite(getattr(item, key)) for key in ("open", "high", "low", "close")}
        if any(value is None or value <= 0 for value in prices.values()):
            raise RuntimeError("ibkr_historical_bar_price_invalid")
        open_px = float(prices["open"])
        high = float(prices["high"])
        low = float(prices["low"])
        close = float(prices["close"])
        if high < max(open_px, close) or low > min(open_px, close):
            raise RuntimeError("ibkr_historical_bar_ohlc_invalid")
        trading_day = local.date().isoformat()
        previous = previous_by_day.get(trading_day)
        rows.append(
            {
                "bar_start": start.isoformat(),
                "bar_end": end.isoformat(),
                "interval_seconds": 300,
                "open": open_px,
                "high": high,
                "low": low,
                "close": close,
                "quality": "ok",
                "gap_before": (previous is not None and start != previous + timedelta(minutes=5)),
                "segment": "rth",
                "trading_date_et": trading_day,
                "contract_identity": contract_identity,
                "contract_identity_ambiguous": False,
            }
        )
        previous_by_day[trading_day] = start
    return sorted(rows, key=lambda row: str(row["bar_start"]))


def _strict_state_snapshot(path: Path) -> tuple[bytes, dict[str, object]]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise RuntimeError("rth_ma_warm_start_state_unreadable") from exc
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("rth_ma_warm_start_state_invalid_json") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("rth_ma_warm_start_state_not_object")
    state = dict(decoded)
    if (
        state.get("schema_version") != SCHEMA_VERSION
        or state.get("interval_seconds") != INTERVAL_SECONDS
    ):
        raise RuntimeError("rth_ma_warm_start_state_schema_mismatch")
    return payload, state


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _assert_worker_inactive(unit: str) -> None:
    result = subprocess.run(
        ["systemctl", "--user", "is-active", unit],
        check=False,
        capture_output=True,
        text=True,
    )
    status = result.stdout.strip()
    if status != "inactive" or result.returncode != 3:
        observed = status or result.stderr.strip() or f"exit_{result.returncode}"
        raise RuntimeError(f"{unit}_must_be_inactive_before_apply:observed={observed}")


@contextmanager
def _stopped_sampler_guard() -> Iterator[None]:
    """Prove the canonical writer is stopped and exclude another sampler."""

    _assert_worker_inactive(DEFAULT_SERVICE_UNIT)
    try:
        with ProcessLock(default_lock_path()):
            _assert_worker_inactive(DEFAULT_SERVICE_UNIT)
            yield
    except ProcessLockUnavailable as exc:
        raise RuntimeError("es_bar_sampler_process_lock_is_held") from exc


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _assert_state_contract_eligible(
    state: dict[str, object],
    *,
    contract_identity: str,
) -> bool:
    """Validate identity evidence; return whether top-level identity needs promotion."""

    active_identity = state.get("contract_identity")
    if active_identity not in (None, "", contract_identity):
        raise RuntimeError("live_state_contract_identity_mismatch")
    current = state.get("current_bar")
    if isinstance(current, dict):
        if current.get("contract_identity_ambiguous") is True:
            raise RuntimeError("live_current_bar_contract_identity_ambiguous")
        current_identity = current.get("contract_identity")
        if current_identity not in (None, "", contract_identity):
            raise RuntimeError("live_current_bar_contract_identity_mismatch")
    closed = state.get("closed_bars")
    if not isinstance(closed, list) or not closed:
        raise RuntimeError("live_state_closed_bars_unavailable")
    return active_identity in (None, "")


def _assert_seed_cutoff(
    rows: list[dict[str, object]],
    *,
    cutoff: datetime,
) -> None:
    if not rows:
        raise RuntimeError("ibkr_historical_rows_empty")
    latest_end = _parse_timestamp(rows[-1].get("bar_end"))
    if latest_end != cutoff:
        raise RuntimeError("ibkr_historical_rows_do_not_reach_cutoff")


def _audit_overlap(
    state: dict[str, object],
    rows: list[dict[str, object]],
    *,
    contract_identity: str,
    minimum: int,
    max_close_difference: float,
    cutoff: datetime,
) -> dict[str, object]:
    if minimum < MIN_APPLY_OVERLAP_BARS:
        raise RuntimeError("live_seed_overlap_minimum_too_low")
    if (
        not math.isfinite(max_close_difference)
        or max_close_difference < 0
        or max_close_difference > MAX_APPLY_OVERLAP_CLOSE_DIFFERENCE
    ):
        raise RuntimeError("live_seed_overlap_close_difference_limit_invalid")
    seed_by_start = {str(row["bar_start"]): row for row in rows[-MAX_RTH_MA_BARS:]}
    closed = state.get("closed_bars")
    if not isinstance(closed, list):
        raise RuntimeError("live_state_closed_bars_unavailable")
    live_rth = sorted(
        (value for value in closed if isinstance(value, dict) and value.get("segment") == "rth"),
        key=lambda value: str(value.get("bar_start") or ""),
    )
    session_day = cutoff.astimezone(NY_TZ).date()
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    if session is None or session.close_at.astimezone(UTC) != cutoff:
        raise RuntimeError("warm_start_cutoff_session_invalid")
    expected = session.expected_five_minute_buckets
    seed_session = [row for row in rows if row.get("trading_date_et") == session_day.isoformat()]
    live_session = [
        row for row in live_rth if row.get("trading_date_et") == session_day.isoformat()
    ]
    if len(seed_session) != expected:
        raise RuntimeError("ibkr_cutoff_session_bucket_count_incomplete")
    if len(live_session) != expected:
        raise RuntimeError("live_cutoff_session_bucket_count_incomplete")
    seed_session_starts = [str(row.get("bar_start") or "") for row in seed_session]
    live_session_starts = [str(row.get("bar_start") or "") for row in live_session]
    if (
        len(set(seed_session_starts)) != expected
        or len(set(live_session_starts)) != expected
        or set(live_session_starts) != set(seed_session_starts)
    ):
        raise RuntimeError("live_cutoff_session_bucket_identity_incomplete")
    if len(live_rth) < minimum:
        raise RuntimeError("live_seed_overlap_below_required_minimum")
    latest_end = _parse_timestamp(live_rth[-1].get("bar_end"))
    if latest_end != cutoff:
        raise RuntimeError("live_rth_bars_do_not_reach_cutoff")

    for value in live_session:
        if (
            value.get("quality") != "ok"
            or value.get("contract_identity_ambiguous") is True
            or value.get("contract_identity") != contract_identity
            or str(value.get("bar_start") or "") not in seed_by_start
        ):
            raise RuntimeError("recent_live_rth_identity_or_quality_unverified")

    overlap_count = 0
    differences: list[float] = []
    for value in live_rth:
        seed = seed_by_start.get(str(value.get("bar_start") or ""))
        if seed is None:
            continue
        if (
            value.get("quality") != "ok"
            or value.get("contract_identity_ambiguous") is True
            or value.get("contract_identity") != contract_identity
        ):
            raise RuntimeError("live_seed_overlap_contract_or_quality_mismatch")
        live_close = _finite(value.get("close"))
        seed_close = _finite(seed.get("close"))
        if live_close is None or seed_close is None:
            raise RuntimeError("live_seed_overlap_close_unavailable")
        overlap_count += 1
        differences.append(abs(live_close - seed_close))
    max_difference = max(differences, default=0.0)
    if overlap_count < minimum:
        raise RuntimeError("live_seed_overlap_below_required_minimum")
    if max_difference > max_close_difference:
        raise RuntimeError("live_seed_overlap_close_difference_too_large")
    return {
        "count": overlap_count,
        "max_close_difference": round(max_difference, 4),
    }


def _assert_ready(
    state: dict[str, object],
    *,
    contract_identity: str,
    cutoff: datetime,
) -> dict[str, object]:
    diagnostics = moving_average_diagnostics(completed_es_bars(state))
    required = (
        "sma200",
        "ma50_slope_3_atr",
        "ma50_slope_6_atr",
        "ma200_slope_3_atr",
        "ma200_slope_6_atr",
        "atr_5m",
    )
    if (
        diagnostics.get("status") != "ready"
        or diagnostics.get("contract_identity") != contract_identity
        or _parse_timestamp(diagnostics.get("latest_bar_end")) != cutoff
        or diagnostics.get("reasons")
        or any(diagnostics.get(key) is None for key in required)
    ):
        raise RuntimeError(
            "rth_ma_warm_start_diagnostics_not_ready:" + json.dumps(diagnostics, sort_keys=True)
        )
    return diagnostics


def _write_backup(path: Path, payload: bytes, *, sha256: str) -> Path:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    directory = path.parent / "state-backups"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    _fsync_directory(path.parent)
    descriptor, backup_name = tempfile.mkstemp(
        prefix=f"{path.name}.{stamp}.{sha256[:12]}.",
        suffix=".bak",
        dir=directory,
    )
    backup = Path(backup_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    _fsync_directory(directory)
    return backup


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_bytes_secure(path: Path, payload: bytes) -> None:
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.restore.",
        suffix=".tmp",
        dir=path.parent,
    )
    temp_path = Path(temp_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temp_path.unlink(missing_ok=True)


def _default_state_path() -> Path:
    storage = StorageSettings.from_env()
    return Path(storage.data_root).expanduser() / "latest" / "es_bars_5m.json"


def main() -> int:
    args = parse_args()
    if args.min_overlap_bars < MIN_APPLY_OVERLAP_BARS:
        raise SystemExit(f"--min-overlap-bars must be at least {MIN_APPLY_OVERLAP_BARS}")
    if (
        not math.isfinite(args.max_overlap_close_difference)
        or args.max_overlap_close_difference < 0
        or args.max_overlap_close_difference > MAX_APPLY_OVERLAP_CLOSE_DIFFERENCE
    ):
        raise SystemExit(
            "--max-overlap-close-difference must be finite and between "
            f"0 and {MAX_APPLY_OVERLAP_CLOSE_DIFFERENCE}"
        )
    identity = _canonical_identity(args.es_expiry)
    now = datetime.now(tz=UTC)
    session_date, cutoff = _latest_completed_session(now)
    state_path = (args.state_path or _default_state_path()).expanduser().resolve()
    original_bytes, original_state = _strict_state_snapshot(state_path)
    original_sha = _sha256(original_bytes)
    promote_identity = _assert_state_contract_eligible(
        original_state,
        contract_identity=identity,
    )

    ib = IB()
    ib.connect(
        args.host,
        args.port,
        clientId=args.client_id,
        readonly=True,
        timeout=10,
    )
    try:
        contract = _qualified_es_contract(ib, expiry=args.es_expiry)
        rows = _request_exact_rth_bars(
            ib,
            contract,
            end_at=cutoff,
            duration=args.duration,
            contract_identity=identity,
        )
    finally:
        ib.disconnect()
    _assert_seed_cutoff(rows, cutoff=cutoff)
    overlap = _audit_overlap(
        original_state,
        rows,
        contract_identity=identity,
        minimum=args.min_overlap_bars,
        max_close_difference=args.max_overlap_close_difference,
        cutoff=cutoff,
    )
    candidate = seed_rth_ma_history(
        original_state,
        rows,
        contract_identity=identity,
        now=now,
        source="ibkr_historical_aggregate_not_live_5s_sampling",
        promote_contract_identity=promote_identity,
    )
    diagnostics = dict(candidate.get("diagnostics") or {})
    diagnostics.update(
        {
            "rth_ma_seed_ibkr_con_id": int(contract.conId),
            "rth_ma_seed_ibkr_local_symbol": str(contract.localSymbol),
            "rth_ma_seed_ibkr_expiry": args.es_expiry,
            "rth_ma_seed_cutoff": cutoff.isoformat(),
            "rth_ma_seed_source_row_count": len(rows),
            "rth_ma_seed_overlap_count": overlap["count"],
            "rth_ma_seed_overlap_max_close_difference": overlap["max_close_difference"],
            "rth_ma_seed_original_state_sha256": original_sha,
        }
    )
    candidate["diagnostics"] = diagnostics
    if len(candidate.get("rth_ma_history") or []) != MAX_RTH_MA_BARS:
        raise RuntimeError("rth_ma_warm_start_requires_full_bounded_history")
    ready = _assert_ready(
        candidate,
        contract_identity=identity,
        cutoff=cutoff,
    )
    approved_keys = {"rth_ma_history", "diagnostics"}
    if promote_identity:
        approved_keys.add("contract_identity")
    immutable_keys = set(original_state) - approved_keys
    if any(candidate.get(key) != original_state.get(key) for key in immutable_keys):
        raise RuntimeError("rth_ma_warm_start_mutated_unapproved_state")

    result: dict[str, object] = {
        "ok": True,
        "applied": False,
        "state_path": str(state_path),
        "session_date": session_date.isoformat(),
        "cutoff": cutoff.isoformat(),
        "contract_identity": identity,
        "qualified_con_id": int(contract.conId),
        "qualified_local_symbol": str(contract.localSymbol),
        "source_row_count": len(rows),
        "seed_bar_count": len(candidate["rth_ma_history"]),
        "overlap": overlap,
        "ma_status": ready["status"],
        "ma_regime_state": ready["regime_state"],
        "sma50": ready["sma50"],
        "sma200": ready["sma200"],
        "atr_5m": ready["atr_5m"],
        "original_state_sha256": original_sha,
    }
    if args.apply:
        with _stopped_sampler_guard(), exclusive_state_lock(state_path):
            current_bytes, current_state = _strict_state_snapshot(state_path)
            if _sha256(current_bytes) != original_sha:
                raise RuntimeError("rth_ma_warm_start_state_changed_after_validation")
            if current_state != original_state:
                raise RuntimeError("rth_ma_warm_start_state_changed_after_validation")
            backup = _write_backup(
                state_path,
                current_bytes,
                sha256=original_sha,
            )
            try:
                atomic_write_json_secure(state_path, candidate)
                _fsync_directory(state_path.parent)
                persisted_bytes, persisted = _strict_state_snapshot(state_path)
                if persisted != candidate:
                    raise RuntimeError("rth_ma_warm_start_persistence_mismatch")
                _assert_ready(
                    persisted,
                    contract_identity=identity,
                    cutoff=cutoff,
                )
                if len(persisted.get("rth_ma_history") or []) != MAX_RTH_MA_BARS:
                    raise RuntimeError("rth_ma_warm_start_persisted_bar_count_invalid")
            except Exception as exc:
                try:
                    _atomic_write_bytes_secure(state_path, current_bytes)
                    restored_bytes, restored = _strict_state_snapshot(state_path)
                    if _sha256(restored_bytes) != original_sha or restored != original_state:
                        raise RuntimeError("rth_ma_warm_start_restore_mismatch")
                except Exception as restore_exc:
                    raise RuntimeError(
                        "rth_ma_warm_start_write_failed_and_restore_failed"
                    ) from restore_exc
                raise RuntimeError("rth_ma_warm_start_write_failed_original_restored") from exc
        result["applied"] = True
        result["backup_path"] = str(backup)
        result["new_state_sha256"] = _sha256(persisted_bytes)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
