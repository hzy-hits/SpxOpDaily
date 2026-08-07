"""Independent, single-writer sampler for canonical ES five-minute bars."""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.market_features.es_bar_state import (
    FUTURE_TOLERANCE_SECONDS,
    INTERVAL_SECONDS,
    SCHEMA_VERSION,
    advance_es_bar_state,
)
from spx_spark.application.market_features.market import normalized_market_sample
from spx_spark.application.runtime.market_features_hot_worker import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ProcessLock,
    ProcessLockUnavailable,
    default_user_runtime_lock_path,
    install_stop_handlers,
    print_event,
)
from spx_spark.config import StorageSettings
from spx_spark.marketdata import as_utc
from spx_spark.settings import load_app_settings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import (
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.storage import LatestState, LatestStateStore


DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_SOURCE_AGE_SECONDS = 15.0
DEFAULT_MAX_READY_AGE_SECONDS = 15.0
LOCK_FILE_NAME = "spx-spark-es-bar-sampler.lock"
STATE_FILE_NAME = "es_bars_5m.json"
LEASE_FILE_NAME = "es_bar_sampler.lease.json"
TASK_NAME = "es_bar_sampler"
LEASE_SCHEMA_VERSION = "es_bar_sampler_lease.v2"
_TRANSIENT_REJECTIONS = frozenset(
    {
        "es_source_timestamp_duplicate_or_out_of_order",
        # A provider projection can omit one observation timestamp while the
        # last accepted ES sample remains fresh. Preserve the prior fenced
        # state for that bounded gap; readiness still fails once either the
        # acceptance or source age exceeds max_source_age_seconds.
        "es_source_timestamp_missing",
    }
)
_SAMPLE_TIMING_FIELDS = (
    "latest_state_load_ms",
    "canonical_cycle_ms",
    "canonical_write_performed",
    "canonical_state_bytes",
)


class CanonicalEsBarStateError(RuntimeError):
    """Raised when canonical bar state cannot safely be advanced."""


def default_lock_path() -> Path:
    return default_user_runtime_lock_path(LOCK_FILE_NAME)


def canonical_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root).expanduser() / "latest" / STATE_FILE_NAME


def lease_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root).expanduser() / "latest" / LEASE_FILE_NAME


def _load_canonical_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalEsBarStateError(
            f"canonical_es_bar_state_unreadable:{type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise CanonicalEsBarStateError("canonical_es_bar_state_not_object")
    if payload and (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("interval_seconds") != INTERVAL_SECONDS
    ):
        raise CanonicalEsBarStateError("canonical_es_bar_state_schema_mismatch")
    return payload


def sample_es_bar_once(
    *,
    storage: StorageSettings,
    policy: MarketFeatureSettings,
    now: datetime | None = None,
    latest_state: LatestState | None = None,
    writer_instance_id: str | None = None,
) -> dict[str, object]:
    """Advance the canonical state from one real latest-state observation.

    The function samples exactly once. It never interpolates observations,
    replays a stale source timestamp, or synthesizes missed five-minute
    buckets.
    """

    observed_at = as_utc(now or datetime.now(tz=timezone.utc))
    instance_id = writer_instance_id or f"direct-{os.getpid()}"
    latest_load_started = time.perf_counter()
    latest = latest_state or LatestStateStore(storage).load(now=observed_at)
    latest_state_load_ms = max(time.perf_counter() - latest_load_started, 0.0) * 1000.0
    sample = normalized_market_sample(latest, now=observed_at, policy=policy)
    path = canonical_state_path(storage)
    canonical_cycle_started = time.perf_counter()
    canonical_write_performed = False
    with exclusive_state_lock(path):
        previous = _load_canonical_state(path)
        state = advance_es_bar_state(
            previous,
            sample,
            now=observed_at,
            policy=policy,
        )
        diagnostics = dict(_mapping(state.get("diagnostics")))
        previous_source_at = previous.get("last_source_at")
        source_at = state.get("last_source_at")
        accepted = bool(source_at and source_at != previous_source_at)
        if accepted:
            diagnostics["canonical_writer"] = TASK_NAME
            diagnostics["writer_pid"] = os.getpid()
            diagnostics["writer_instance_id"] = instance_id
            state["diagnostics"] = diagnostics
            state["writer_instance_id"] = instance_id
            atomic_write_json_secure(path, state)
            canonical_write_performed = True

    canonical_cycle_ms = max(time.perf_counter() - canonical_cycle_started, 0.0) * 1000.0
    current = _mapping(state.get("current_bar"))
    return {
        "ok": True,
        "at": observed_at.isoformat(),
        "accepted": accepted,
        "source_at": source_at,
        "provider": state.get("last_provider"),
        "bar_start": current.get("bar_start"),
        "sample_count": current.get("sample_count", 0),
        "closed_bar_count": len(_rows(state.get("closed_bars"))),
        "rejection": diagnostics.get("last_rejection"),
        "writer_instance_id": instance_id,
        "latest_state_load_ms": latest_state_load_ms,
        "canonical_cycle_ms": canonical_cycle_ms,
        "canonical_write_performed": canonical_write_performed,
        "canonical_state_bytes": path.stat().st_size if path.is_file() else 0,
    }


def run_es_bar_sample_cycle(
    *,
    storage: StorageSettings,
    policy: MarketFeatureSettings,
    writer_instance_id: str,
    clock: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
) -> dict[str, object]:
    if not policy.enabled:
        at = as_utc(clock())
        return {
            "ok": True,
            "at": at.isoformat(),
            "accepted": False,
            "source_at": None,
            "rejection": "market_features_disabled",
            "writer_instance_id": writer_instance_id,
        }
    return sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=clock(),
        writer_instance_id=writer_instance_id,
    )


def run_es_bar_sampler_loop(
    cycle: Callable[[], Mapping[str, object]],
    *,
    interval_seconds: float,
    stop_event: threading.Event,
    writer_instance_id: str,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    max_cycles: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utcnow: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    emit: Callable[[dict[str, object]], None] = print_event,
    output_lease_path: Path | None = None,
) -> int:
    """Run sampler cycles while keeping liveness and data freshness separate."""

    if interval_seconds <= 0:
        raise ValueError("sampler interval must be positive")
    if max_source_age_seconds <= 0:
        raise ValueError("max source age must be positive")
    if max_consecutive_failures <= 0:
        raise ValueError("max consecutive failures must be positive")
    if max_cycles is not None and max_cycles <= 0:
        raise ValueError("max cycles must be positive when provided")
    if not writer_instance_id:
        raise ValueError("writer instance id must not be empty")

    cycle_number = 0
    consecutive_failures = 0
    consecutive_data_failures = 0
    last_accepted_at: datetime | None = None
    last_accepted_source_at: datetime | None = None
    while not stop_event.is_set():
        cycle_number += 1
        started_at = as_utc(utcnow())
        started_monotonic = monotonic()
        error: str | None = None
        sample_result: Mapping[str, object] = {}
        try:
            sample_result = cycle()
        except Exception as exc:  # noqa: BLE001 - isolate one observation cycle
            error = f"{type(exc).__name__}:{exc}"
        finished_monotonic = monotonic()
        finished_at = as_utc(utcnow())
        duration_seconds = max(finished_monotonic - started_monotonic, 0.0)
        liveness_ok = error is None
        consecutive_failures = 0 if liveness_ok else consecutive_failures + 1

        source_at = _parse_at(sample_result.get("source_at"))
        accepted = bool(sample_result.get("accepted") is True and source_at is not None)
        rejection_value = sample_result.get("rejection")
        rejection = (
            str(rejection_value) if isinstance(rejection_value, str) and rejection_value else None
        )
        sample_writer_instance_id = _nonempty_text(sample_result.get("writer_instance_id"))
        if sample_result.get("accepted") is True and source_at is None:
            rejection = "accepted_source_timestamp_missing"
        if (
            error is None
            and sample_writer_instance_id is not None
            and sample_writer_instance_id != writer_instance_id
        ):
            rejection = "writer_instance_mismatch"
            accepted = False
        if not accepted and rejection is None:
            rejection = "sampler_observation_not_accepted"

        if accepted:
            last_accepted_at = finished_at
            last_accepted_source_at = source_at
            consecutive_data_failures = 0
        else:
            consecutive_data_failures += 1

        source_age_seconds = _age_seconds(finished_at, source_at)
        last_accepted_age_seconds = _age_seconds(finished_at, last_accepted_at)
        last_accepted_source_age_seconds = _age_seconds(
            finished_at,
            last_accepted_source_at,
        )
        source_fresh = _fresh_age(
            last_accepted_source_age_seconds,
            max_age_seconds=max_source_age_seconds,
        )
        last_accept_fresh = _fresh_age(
            last_accepted_age_seconds,
            max_age_seconds=max_source_age_seconds,
            future_tolerance_seconds=0.0,
        )
        hard_rejection = rejection is not None and rejection not in _TRANSIENT_REJECTIONS
        data_healthy = bool(
            liveness_ok and source_fresh and last_accept_fresh and not hard_rejection
        )
        overrun_seconds = max(duration_seconds - interval_seconds, 0.0)
        sla_ok = overrun_seconds <= 0.0
        ok = bool(liveness_ok and data_healthy and sla_ok)
        event: dict[str, object] = {
            "schema_version": LEASE_SCHEMA_VERSION,
            "task": TASK_NAME,
            "event": "cycle_finished",
            "cycle": cycle_number,
            "ok": ok,
            "liveness_ok": liveness_ok,
            "data_healthy": data_healthy,
            "sla_ok": sla_ok,
            "error": error,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": duration_seconds * 1000.0,
            "cycle_sla_ms": interval_seconds * 1000.0,
            "interval_seconds": interval_seconds,
            "overrun_ms": overrun_seconds * 1000.0,
            "writer_instance_id": writer_instance_id,
            "writer_has_accepted": last_accepted_source_at is not None,
            "accepted": accepted,
            "source_at": source_at.isoformat() if source_at is not None else None,
            "source_age_seconds": source_age_seconds,
            "last_accepted_at": (
                last_accepted_at.isoformat() if last_accepted_at is not None else None
            ),
            "last_accepted_source_at": (
                last_accepted_source_at.isoformat() if last_accepted_source_at is not None else None
            ),
            "last_accepted_age_seconds": last_accepted_age_seconds,
            "last_accepted_source_age_seconds": last_accepted_source_age_seconds,
            "max_source_age_seconds": max_source_age_seconds,
            "rejection": rejection,
            "consecutive_failures": consecutive_failures,
            "consecutive_data_failures": consecutive_data_failures,
        }
        for field in _SAMPLE_TIMING_FIELDS:
            value = sample_result.get(field)
            if value is not None:
                event[field] = value
        if output_lease_path is not None:
            atomic_write_json_secure(output_lease_path, event)
        emit(event)
        if consecutive_failures >= max_consecutive_failures:
            return 1
        if max_cycles is not None and cycle_number >= max_cycles:
            break

        remaining = max(interval_seconds - duration_seconds, 0.0)
        if stop_event.wait(remaining):
            break
    return 0


def sampler_readiness(
    *,
    storage: StorageSettings,
    now: datetime | None = None,
    max_age_seconds: float = DEFAULT_MAX_READY_AGE_SECONDS,
) -> dict[str, object]:
    """Verify a fresh accepted observation fenced to the currently leased writer."""

    checked_at = as_utc(now or datetime.now(tz=timezone.utc))
    reasons: list[str] = []
    lease = read_json_object(lease_path(storage))
    try:
        state = _load_canonical_state(canonical_state_path(storage))
    except CanonicalEsBarStateError as exc:
        state = {}
        reasons.append(str(exc))

    if lease.get("schema_version") != LEASE_SCHEMA_VERSION:
        reasons.append("lease_schema_invalid")
    if lease.get("task") != TASK_NAME or lease.get("event") != "cycle_finished":
        reasons.append("lease_identity_invalid")
    if lease.get("ok") is not True:
        reasons.append("lease_not_healthy")
    if lease.get("liveness_ok") is not True:
        reasons.append("lease_liveness_not_healthy")
    if lease.get("data_healthy") is not True:
        reasons.append("lease_data_not_healthy")
    if lease.get("sla_ok") is not True:
        reasons.append("lease_cycle_sla_failed")
    if lease.get("writer_has_accepted") is not True:
        reasons.append("writer_has_not_accepted")

    writer_instance_id = _nonempty_text(lease.get("writer_instance_id"))
    state_writer_instance_id = _nonempty_text(state.get("writer_instance_id"))
    diagnostics = _mapping(state.get("diagnostics"))
    if writer_instance_id is None:
        reasons.append("lease_writer_instance_missing")
    if state_writer_instance_id is None:
        reasons.append("state_writer_instance_missing")
    if (
        writer_instance_id is not None
        and state_writer_instance_id is not None
        and writer_instance_id != state_writer_instance_id
    ):
        reasons.append("writer_instance_mismatch")
    if diagnostics.get("canonical_writer") != TASK_NAME:
        reasons.append("canonical_writer_invalid")
    if diagnostics.get("writer_instance_id") != state_writer_instance_id:
        reasons.append("state_writer_fence_invalid")

    finished_at = _parse_at(lease.get("finished_at"))
    last_accepted_at = _parse_at(lease.get("last_accepted_at"))
    last_accepted_source_at = _parse_at(lease.get("last_accepted_source_at"))
    state_source_at = _parse_at(state.get("last_source_at"))
    lease_age_seconds = _age_seconds(checked_at, finished_at)
    last_accepted_age_seconds = _age_seconds(checked_at, last_accepted_at)
    source_age_seconds = _age_seconds(checked_at, last_accepted_source_at)
    if not _fresh_age(lease_age_seconds, max_age_seconds=max_age_seconds):
        reasons.append("lease_stale")
    if not _fresh_age(
        last_accepted_age_seconds,
        max_age_seconds=max_age_seconds,
        future_tolerance_seconds=FUTURE_TOLERANCE_SECONDS,
    ):
        reasons.append("last_accept_stale")
    if not _fresh_age(source_age_seconds, max_age_seconds=max_age_seconds):
        reasons.append("accepted_source_stale")
    if last_accepted_source_at is None or state_source_at != last_accepted_source_at:
        reasons.append("accepted_source_marker_mismatch")

    return {
        "schema_version": LEASE_SCHEMA_VERSION,
        "task": TASK_NAME,
        "event": "ready_check",
        "ok": not reasons,
        "ready": not reasons,
        "checked_at": checked_at.isoformat(),
        "max_age_seconds": max_age_seconds,
        "writer_instance_id": writer_instance_id,
        "state_writer_instance_id": state_writer_instance_id,
        "lease_age_seconds": lease_age_seconds,
        "last_accepted_age_seconds": last_accepted_age_seconds,
        "source_age_seconds": source_age_seconds,
        "reasons": reasons,
    }


def mark_sampler_starting(
    *,
    storage: StorageSettings,
    writer_instance_id: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Invalidate any prior ready lease before a new writer starts."""

    at = as_utc(now or datetime.now(tz=timezone.utc))
    event: dict[str, object] = {
        "schema_version": LEASE_SCHEMA_VERSION,
        "task": TASK_NAME,
        "event": "starting",
        "ok": False,
        "liveness_ok": True,
        "data_healthy": False,
        "sla_ok": False,
        "status": "initializing",
        "started_at": at.isoformat(),
        "finished_at": None,
        "writer_instance_id": writer_instance_id,
        "writer_has_accepted": False,
        "accepted": False,
        "source_at": None,
        "last_accepted_at": None,
        "last_accepted_source_at": None,
        "rejection": "writer_initializing",
        "consecutive_failures": 0,
        "consecutive_data_failures": 0,
    }
    atomic_write_json_secure(lease_path(storage), event)
    return event


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the independent canonical ES five-minute bar sampler."
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Start-to-start sampling cadence; defaults to five seconds.",
    )
    parser.add_argument(
        "--lock-path",
        type=Path,
        help="Exclusive process-owner lock; defaults to the XDG user-runtime path.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_FAILURES,
    )
    parser.add_argument(
        "--max-source-age-seconds",
        type=float,
        default=DEFAULT_MAX_SOURCE_AGE_SECONDS,
        help="Maximum accepted ES source and observation age for a healthy lease.",
    )
    parser.add_argument(
        "--max-ready-age-seconds",
        type=float,
        default=DEFAULT_MAX_READY_AGE_SECONDS,
        help="Maximum lease, acceptance, and ES source age for --check-ready.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one locked sample and exit.")
    mode.add_argument(
        "--check-ready",
        action="store_true",
        help="Validate a fresh accepted sample fenced to the current writer.",
    )
    mode.add_argument(
        "--mark-starting",
        action="store_true",
        help="Invalidate a prior ready lease before systemd starts a new writer.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage = StorageSettings.from_env()
    if args.check_ready:
        readiness = sampler_readiness(
            storage=storage,
            max_age_seconds=float(args.max_ready_age_seconds),
        )
        print_event(readiness)
        return 0 if readiness["ready"] is True else 1
    if args.mark_starting:
        print_event(
            mark_sampler_starting(
                storage=storage,
                writer_instance_id=f"prestart-{uuid.uuid4().hex}",
            )
        )
        return 0

    stop_event = threading.Event()
    install_stop_handlers(stop_event)
    return run_with_stop(
        stop_event=stop_event,
        interval_seconds=float(args.interval_seconds),
        lock_path=args.lock_path,
        max_source_age_seconds=float(args.max_source_age_seconds),
        max_consecutive_failures=args.max_consecutive_failures,
        max_cycles=1 if args.once else None,
    )


def run_with_stop(
    *,
    stop_event: threading.Event,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    lock_path: Path | None = None,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    max_cycles: int | None = None,
) -> int:
    storage = StorageSettings.from_env()
    policy = load_app_settings().market_features
    lock_path = lock_path or default_lock_path()
    writer_instance_id = uuid.uuid4().hex

    def cycle() -> Mapping[str, object]:
        return run_es_bar_sample_cycle(
            storage=storage,
            policy=policy,
            writer_instance_id=writer_instance_id,
        )

    try:
        with ProcessLock(lock_path):
            starting_event = mark_sampler_starting(
                storage=storage,
                writer_instance_id=writer_instance_id,
            )
            print_event(
                {
                    **starting_event,
                    "event": "started",
                    "pid": os.getpid(),
                    "interval_seconds": interval_seconds,
                    "lock_path": str(lock_path),
                    "state_path": str(canonical_state_path(storage)),
                }
            )
            exit_code = run_es_bar_sampler_loop(
                cycle,
                interval_seconds=interval_seconds,
                stop_event=stop_event,
                writer_instance_id=writer_instance_id,
                max_source_age_seconds=max_source_age_seconds,
                max_consecutive_failures=max_consecutive_failures,
                max_cycles=max_cycles,
                output_lease_path=lease_path(storage),
            )
    except ProcessLockUnavailable as exc:
        print_event(
            {
                "task": TASK_NAME,
                "event": "lock_unavailable",
                "ok": False,
                "error": str(exc),
                "lock_path": str(lock_path),
            }
        )
        return 75

    print_event(
        {
            "task": TASK_NAME,
            "event": "stopped",
            "ok": exit_code == 0,
            "exit_code": exit_code,
            "writer_instance_id": writer_instance_id,
        }
    )
    return exit_code


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _parse_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return as_utc(parsed)


def _age_seconds(now: datetime, then: datetime | None) -> float | None:
    if then is None:
        return None
    return (as_utc(now) - as_utc(then)).total_seconds()


def _fresh_age(
    age_seconds: float | None,
    *,
    max_age_seconds: float,
    future_tolerance_seconds: float = FUTURE_TOLERANCE_SECONDS,
) -> bool:
    return bool(
        age_seconds is not None and -future_tolerance_seconds <= age_seconds <= max_age_seconds
    )


def _nonempty_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
