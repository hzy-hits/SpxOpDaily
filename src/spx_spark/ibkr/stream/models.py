"""Typed models and timing constants for the IBKR stream runtime."""

from __future__ import annotations

import os
import random
import tempfile
import time
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from math import ceil, isfinite, log2
from pathlib import Path

from spx_spark.config import IbkrSettings
from spx_spark.sampling import OptionContractSpec

MAX_TRACKED_ERRORS = 200
SUBSCRIPTION_CONFIRM_SECONDS = 0.5
SUBSCRIPTION_REJECTION_CODES = frozenset({100, 101, 200, 354, 420, 10197})
OPTION_ROTATION_RETRY_SECONDS = 30.0
QUALIFICATION_TIMEOUT_SECONDS = 5.0
HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS = 6.0
HOT_FLUSH_SLEEP_MAX_SECONDS = 5.0
OPTION_CACHE_TTL_SECONDS = 900.0


class StreamAction(str, Enum):
    CONTINUE = "continue"
    RECONNECT = "reconnect"
    CONFLICT_WAIT = "conflict_wait"
    POLICY_BLOCKED = "policy_blocked"
    GATEWAY_RESTART = "gateway_restart"


@dataclass
class CompetingSessionCircuit:
    """Deterministic exponential cooldown for IBKR error 10197.

    The circuit never preempts another IBKR session.  It only controls when
    this collector may make its next ordinary, read-only market-data attempt.
    A healthy probe starts a recovery window; it does not erase conflict
    history immediately. Only continuously healthy data for
    ``recovery_seconds`` closes the circuit, so an intermittent entitlement
    owner cannot keep every retry at the minimum delay.
    """

    min_seconds: float
    max_seconds: float
    recovery_seconds: float = 8.0
    failures: int = 0
    retry_not_before: float = 0.0
    recovery_started_at: float | None = None

    def __post_init__(self) -> None:
        if self.min_seconds <= 0:
            raise ValueError("competing-session cooldown minimum must be positive")
        if self.max_seconds < self.min_seconds:
            raise ValueError("competing-session cooldown maximum cannot be below minimum")
        if self.recovery_seconds <= 0:
            raise ValueError("competing-session recovery window must be positive")

    def open(self, *, now_monotonic: float) -> float:
        max_doublings = max(ceil(log2(self.max_seconds / self.min_seconds)), 0)
        exponent = min(self.failures, max_doublings)
        delay = min(self.min_seconds * (2**exponent), self.max_seconds)
        self.failures += 1
        self.retry_not_before = now_monotonic + delay
        self.recovery_started_at = None
        return delay

    def observe_healthy(self, *, now_monotonic: float) -> bool:
        """Close only after usable data remains stable for the recovery window.

        Returns ``True`` exactly when this observation closes a previously
        opened circuit.
        """

        if self.failures == 0:
            return False
        if self.recovery_started_at is None:
            self.recovery_started_at = now_monotonic
            return False
        if now_monotonic - self.recovery_started_at < self.recovery_seconds:
            return False
        self.close()
        return True

    def interrupt_recovery(self) -> None:
        """Restart the stability window without erasing conflict history."""

        self.recovery_started_at = None

    def remaining_seconds(self, *, now_monotonic: float) -> float:
        return max(self.retry_not_before - now_monotonic, 0.0)

    def state(self, *, now_monotonic: float) -> str:
        if self.failures == 0:
            return "closed"
        if self.remaining_seconds(now_monotonic=now_monotonic) > 0:
            return "open"
        if self.recovery_started_at is not None:
            return "recovering"
        return "half_open"

    def close(self) -> None:
        self.failures = 0
        self.retry_not_before = 0.0
        self.recovery_started_at = None


@dataclass
class ReconnectPolicy:
    min_seconds: float
    max_seconds: float
    attempt: int = 0

    def next_delay(self) -> float:
        base = min(self.min_seconds * (2**self.attempt), self.max_seconds)
        self.attempt += 1
        # Jitter avoids synchronized reconnect storms after a shared disconnect;
        # cap again so jittered values never exceed max_seconds.
        return min(reconnect_jitter(base), self.max_seconds)

    def reset(self) -> None:
        self.attempt = 0


def reconnect_jitter(seconds: float) -> float:
    """Scale a backoff delay by a random factor in [0.5, 1.5)."""

    return seconds * random.uniform(0.5, 1.5)


def lifecycle_has_qualification_budget(
    started_at: float,
    *,
    now_monotonic: float | None = None,
) -> bool:
    """Keep lifecycle work bounded so persisted hot rows stay <=12s apart."""

    now = time.monotonic() if now_monotonic is None else now_monotonic
    remaining = HOT_FLUSH_LIFECYCLE_BUDGET_SECONDS - max(now - started_at, 0.0)
    return remaining >= QUALIFICATION_TIMEOUT_SECONDS + SUBSCRIPTION_CONFIRM_SECONDS


def effective_hot_flush_sleep_seconds(configured_seconds: float) -> float:
    """Honor faster flush settings while enforcing the reliability ceiling."""

    return min(max(float(configured_seconds), 0.0), HOT_FLUSH_SLEEP_MAX_SECONDS)


@dataclass(frozen=True)
class OptionSubscriptionPlan:
    """Line-budgeted view of a sampling plan.

    `hot` stays subscribed for the lifetime of the plan; `rotations` are
    swapped in one slice at a time, each slice fitting the leftover budget.
    """

    atm_strike: int
    expiry: str
    hot: tuple[OptionContractSpec, ...]
    rotations: tuple[tuple[OptionContractSpec, ...], ...]

    @property
    def rotation_count(self) -> int:
        return len(self.rotations)


CONFLICT_OVERLAY_RELATIVE = Path("runtime") / "ibkr_conflict.toml"


@dataclass(frozen=True, slots=True)
class ConflictOverlay:
    recovery_seconds: float
    probe_seconds: float
    probe_max_seconds: float


def conflict_overlay_path(data_root: object) -> Path | None:
    if not data_root:
        return None
    return Path(str(data_root)).expanduser() / CONFLICT_OVERLAY_RELATIVE


def parse_conflict_overlay(payload: Mapping[str, object]) -> ConflictOverlay | None:
    recovery = _overlay_positive_seconds(payload.get("recovery_seconds"))
    probe = _overlay_positive_seconds(payload.get("probe_seconds"))
    probe_max = _overlay_positive_seconds(payload.get("probe_max_seconds"))
    if recovery is None or probe is None or probe_max is None:
        return None
    if probe_max < probe:
        return None
    return ConflictOverlay(
        recovery_seconds=recovery,
        probe_seconds=probe,
        probe_max_seconds=probe_max,
    )


def load_conflict_overlay(
    path: Path,
    *,
    previous_mtime: float | None = None,
) -> tuple[ConflictOverlay | None, float | None, str | None]:
    """Load the 10197 overlay if it changed.

    Returns ``(overlay, mtime, error)``. ``overlay`` is ``None`` when the file
    is missing, unchanged, or invalid. Invalid files keep the previous circuit
    values and return a reason in ``error``.
    """

    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None, None, None
    except OSError as exc:
        return None, previous_mtime, f"stat failed: {exc}"
    if previous_mtime is not None and mtime == previous_mtime:
        return None, mtime, None
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return None, mtime, f"invalid overlay: {exc}"
    if not isinstance(payload, dict):
        return None, mtime, "overlay root must be a table"
    overlay = parse_conflict_overlay(payload)
    if overlay is None:
        return None, mtime, "overlay needs positive recovery/probe seconds"
    return overlay, mtime, None


def render_conflict_overlay(overlay: ConflictOverlay) -> str:
    return (
        "# Hot-reloaded by spx-spark-ibkr-stream on each flush.\n"
        "# Edit this file to change 10197 recovery without restarting the collector.\n"
        "schema_version = 1\n"
        f"recovery_seconds = {overlay.recovery_seconds:g}\n"
        f"probe_seconds = {overlay.probe_seconds:g}\n"
        f"probe_max_seconds = {overlay.probe_max_seconds:g}\n"
    )


def seed_conflict_overlay(path: Path, overlay: ConflictOverlay) -> bool:
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = render_conflict_overlay(overlay)
    file_descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temp_path = Path(temp_name)
    try:
        os.fchmod(file_descriptor, 0o600)
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            file_descriptor = -1
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        temp_path.unlink(missing_ok=True)
    return True


def apply_conflict_overlay(
    circuit: CompetingSessionCircuit, overlay: ConflictOverlay
) -> bool:
    changed = (
        circuit.recovery_seconds != overlay.recovery_seconds
        or circuit.min_seconds != overlay.probe_seconds
        or circuit.max_seconds != overlay.probe_max_seconds
    )
    circuit.recovery_seconds = overlay.recovery_seconds
    circuit.min_seconds = overlay.probe_seconds
    circuit.max_seconds = overlay.probe_max_seconds
    return changed


def _overlay_positive_seconds(raw: object) -> float | None:
    if isinstance(raw, Mapping):
        raw = raw.get("value")
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        return None
    value = float(raw)
    if not isfinite(value) or value <= 0:
        return None
    return value


def replace_client_id(settings: IbkrSettings, client_id: int) -> IbkrSettings:
    from dataclasses import asdict

    payload = asdict(settings)
    payload["client_id"] = client_id
    return IbkrSettings(**payload)
