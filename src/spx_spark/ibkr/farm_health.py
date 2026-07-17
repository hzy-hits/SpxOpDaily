"""IBKR data-farm health tracking and controlled Gateway recovery.

Port-level watchdogs miss the failure mode where Gateway accepts API
connections but IBKR farms (sec-def, HMDS, market data) are disconnected.
This module tracks farm status from IBKR error codes, probes the data plane
after connect, and can request a user-scoped ``ibc-gateway`` restart when a
broken state persists.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from spx_spark.config import IbkrSettings, RuntimePolicySettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.runtime_mode import load_override


FARM_OK_CODES = frozenset({2104, 2106, 2158})
FARM_CONNECTING_CODES = frozenset({2119})
FARM_BROKEN_CODES = frozenset({2103, 2110, 2157})
TWS_CONNECTIVITY_LOST_CODES = frozenset({1100, 2110})
TWS_CONNECTIVITY_RESTORED_CODES = frozenset({1101, 1102})

NON_DEGRADING_ERROR_CODES = FARM_OK_CODES | FARM_CONNECTING_CODES | TWS_CONNECTIVITY_RESTORED_CODES

DATA_FLOW_SILENCE_FARM = "data-flow-silence"


def data_flow_silence_breached(
    *,
    ticker_time: datetime | None,
    now: datetime,
    silence_seconds: float,
) -> bool:
    """True when subscribed market data stopped ticking during an open session.

    The connection can stay alive while a farm silently stops delivering
    ticks (zombie session): no error codes, ``reqCurrentTime`` fine, quotes
    frozen. ES is the canary because it trades nearly 24h on weekdays. The
    session must have been open across the whole silent window so the
    expected quiet around the daily Globex break and weekends never fires.
    """

    if ticker_time is None or silence_seconds <= 0:
        return False
    if (now - ticker_time).total_seconds() <= silence_seconds:
        return False
    window_start = now - timedelta(seconds=silence_seconds)
    return DEFAULT_MARKET_CALENDAR.is_globex_open(now) and DEFAULT_MARKET_CALENDAR.is_globex_open(
        window_start
    )

_FARM_NAME_RE = re.compile(
    r"(?:market data farm(?: connection)? is|hmds data farm connection is|sec-def data farm connection is)"
    r" ([^:]+):(\S+)",
    re.IGNORECASE,
)


class FarmLinkStatus(str, Enum):
    OK = "ok"
    CONNECTING = "connecting"
    BROKEN = "broken"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FarmStatusEvent:
    status: FarmLinkStatus
    error_code: int | None = None
    message: str | None = None
    farm: str | None = None
    broken_seconds: float | None = None

    def to_log_event(self, *, task: str = "ibkr_farm") -> dict[str, object]:
        payload: dict[str, object] = {
            "task": task,
            "event": "farm_status",
            "farm_status": self.status.value,
            "ts": datetime.now(tz=timezone.utc).isoformat(),
        }
        if self.error_code is not None:
            payload["error_code"] = self.error_code
        if self.message:
            payload["message"] = self.message
        if self.farm:
            payload["farm"] = self.farm
        if self.broken_seconds is not None:
            payload["broken_seconds"] = round(self.broken_seconds, 1)
        return payload


@dataclass(frozen=True)
class DataPlaneProbeResult:
    ok: bool
    current_time_ok: bool
    qualify_ok: bool
    error: str | None = None
    current_time: str | None = None
    contract: str | None = None

    def to_log_event(self, *, task: str = "ibkr_stream") -> dict[str, object]:
        return {
            "task": task,
            "event": "data_plane_probe",
            "ok": self.ok,
            "current_time_ok": self.current_time_ok,
            "qualify_ok": self.qualify_ok,
            "error": self.error,
            "current_time": self.current_time,
            "contract": self.contract,
            "ts": datetime.now(tz=timezone.utc).isoformat(),
        }


def classify_farm_error(error_code: int, message: str) -> tuple[FarmLinkStatus, str | None]:
    if error_code in TWS_CONNECTIVITY_LOST_CODES:
        return FarmLinkStatus.BROKEN, "tws-server"
    if error_code in TWS_CONNECTIVITY_RESTORED_CODES:
        return FarmLinkStatus.OK, "tws-server"
    if error_code in FARM_OK_CODES or "connection is ok" in message.lower():
        farm = parse_farm_name(message)
        return FarmLinkStatus.OK, farm
    if error_code in FARM_CONNECTING_CODES or "is connecting" in message.lower():
        return FarmLinkStatus.CONNECTING, parse_farm_name(message)
    if error_code in FARM_BROKEN_CODES or "is broken" in message.lower():
        return FarmLinkStatus.BROKEN, parse_farm_name(message)
    if error_code == 2110:
        return FarmLinkStatus.BROKEN, "tws-server"
    return FarmLinkStatus.UNKNOWN, parse_farm_name(message)


def parse_farm_name(message: str) -> str | None:
    match = _FARM_NAME_RE.search(message)
    if match:
        return match.group(2).split(".", maxsplit=1)[0]
    lowered = message.lower()
    if "between trader workstation and server is broken" in lowered:
        return "tws-server"
    if "hmds data farm connection is inactive" in lowered:
        return "hmds"
    return None


def runtime_blocks_gateway_restart(
    runtime_policy: RuntimePolicySettings,
    *,
    force: bool = False,
) -> bool:
    if force:
        return False
    override = load_override(runtime_policy.runtime_mode_path)
    return override is not None and override.mode == "protected"


def probe_data_plane(ib: Any, settings: IbkrSettings) -> DataPlaneProbeResult:
    from ib_async import Future

    current_time_ok = False
    current_time: str | None = None
    try:
        server_time = ib.reqCurrentTime()
        current_time_ok = server_time is not None
        if server_time is not None:
            current_time = server_time.astimezone(timezone.utc).isoformat()
    except Exception as exc:  # noqa: BLE001
        return DataPlaneProbeResult(
            ok=False,
            current_time_ok=False,
            qualify_ok=False,
            error=f"reqCurrentTime failed: {exc}",
        )

    contract = Future("ES", settings.es_expiry, "CME", currency="USD")
    try:
        qualified = ib.qualifyContracts(contract)
        qualify_ok = bool(qualified and getattr(qualified[0], "conId", 0))
        if not qualify_ok:
            return DataPlaneProbeResult(
                ok=False,
                current_time_ok=current_time_ok,
                qualify_ok=False,
                error="qualifyContracts returned no ES contract",
                current_time=current_time,
                contract=str(contract),
            )
    except Exception as exc:  # noqa: BLE001
        return DataPlaneProbeResult(
            ok=False,
            current_time_ok=current_time_ok,
            qualify_ok=False,
            error=f"qualifyContracts failed: {exc}",
            current_time=current_time,
            contract=str(contract),
        )

    return DataPlaneProbeResult(
        ok=True,
        current_time_ok=True,
        qualify_ok=True,
        current_time=current_time,
        contract=str(contract),
    )


@dataclass
class FarmHealthTracker:
    """Tracks farm health transitions and sustained broken duration."""

    broken_restart_seconds: float = 180.0
    status: FarmLinkStatus = FarmLinkStatus.UNKNOWN
    broken_since: float | None = None
    last_error_code: int | None = None
    last_error_message: str | None = None
    last_farm: str | None = None
    farms: dict[str, FarmLinkStatus] = field(default_factory=dict)
    broken_since_by_farm: dict[str, float] = field(default_factory=dict)

    def _refresh_aggregate_status(self) -> None:
        broken_farms = {
            farm for farm, status in self.farms.items() if status is FarmLinkStatus.BROKEN
        }
        for farm in tuple(self.broken_since_by_farm):
            if farm not in broken_farms:
                self.broken_since_by_farm.pop(farm, None)

        if broken_farms:
            self.status = FarmLinkStatus.BROKEN
            self.broken_since = min(self.broken_since_by_farm[farm] for farm in broken_farms)
            return

        self.broken_since = None
        if any(status is FarmLinkStatus.CONNECTING for status in self.farms.values()):
            self.status = FarmLinkStatus.CONNECTING
        elif self.farms:
            self.status = FarmLinkStatus.OK
        else:
            self.status = FarmLinkStatus.UNKNOWN

    def observe(
        self,
        error_code: int,
        message: str,
        *,
        now: float | None = None,
    ) -> FarmStatusEvent | None:
        if now is None:
            now = time.monotonic()
        link_status, farm = classify_farm_error(error_code, message)
        if link_status is FarmLinkStatus.UNKNOWN:
            return None

        previous = self.status
        self.last_error_code = error_code
        self.last_error_message = message
        self.last_farm = farm

        if farm:
            self.farms[farm] = link_status
            if link_status is FarmLinkStatus.BROKEN:
                self.broken_since_by_farm.setdefault(farm, now)
            elif link_status is FarmLinkStatus.OK:
                self.broken_since_by_farm.pop(farm, None)
        elif link_status is FarmLinkStatus.BROKEN:
            # Unknown broken events must remain active until the session resets;
            # an unrelated farm recovery cannot safely identify what recovered.
            farm = "unknown-farm"
            self.farms[farm] = link_status
            self.broken_since_by_farm.setdefault(farm, now)

        self._refresh_aggregate_status()

        if self.status == previous:
            return None

        broken_seconds = None
        if self.broken_since is not None:
            broken_seconds = now - self.broken_since
        return FarmStatusEvent(
            status=self.status,
            error_code=error_code,
            message=message,
            farm=farm,
            broken_seconds=broken_seconds,
        )

    def mark_probe_failed(
        self, probe: DataPlaneProbeResult, *, now: float | None = None
    ) -> FarmStatusEvent:
        if now is None:
            now = time.monotonic()
        self.last_error_code = None
        self.last_error_message = probe.error
        self.last_farm = "data-plane-probe"
        self.farms["data-plane-probe"] = FarmLinkStatus.BROKEN
        self.broken_since_by_farm.setdefault("data-plane-probe", now)
        self._refresh_aggregate_status()
        return FarmStatusEvent(
            status=self.status,
            message=probe.error,
            farm="data-plane-probe",
            broken_seconds=0.0,
        )

    def mark_probe_succeeded(self) -> None:
        """Clear only a prior data-plane probe failure."""

        if "data-plane-probe" not in self.farms:
            return
        self.farms["data-plane-probe"] = FarmLinkStatus.OK
        self.broken_since_by_farm.pop("data-plane-probe", None)
        self._refresh_aggregate_status()

    def mark_data_flow_silent(
        self, message: str, *, now: float | None = None
    ) -> FarmStatusEvent | None:
        """Register a zombie-session detection: subscribed, but no ticks.

        Mirrors the data-plane probe path with its own synthetic farm so a
        silent freeze accumulates broken duration towards the gateway
        restart gate exactly like an explicit farm error would.
        """

        if now is None:
            now = time.monotonic()
        previous = self.status
        self.last_error_code = None
        self.last_error_message = message
        self.last_farm = DATA_FLOW_SILENCE_FARM
        self.farms[DATA_FLOW_SILENCE_FARM] = FarmLinkStatus.BROKEN
        self.broken_since_by_farm.setdefault(DATA_FLOW_SILENCE_FARM, now)
        self._refresh_aggregate_status()
        if self.status == previous:
            return None
        return FarmStatusEvent(
            status=self.status,
            message=message,
            farm=DATA_FLOW_SILENCE_FARM,
            broken_seconds=0.0,
        )

    def mark_data_flow_live(self) -> None:
        """Clear only a prior data-flow silence detection."""

        if DATA_FLOW_SILENCE_FARM not in self.farms:
            return
        self.farms[DATA_FLOW_SILENCE_FARM] = FarmLinkStatus.OK
        self.broken_since_by_farm.pop(DATA_FLOW_SILENCE_FARM, None)
        self._refresh_aggregate_status()

    def broken_duration(self, *, now: float | None = None) -> float | None:
        if self.broken_since is None:
            return None
        if now is None:
            now = time.monotonic()
        return now - self.broken_since

    def should_restart_gateway(self, *, now: float | None = None) -> bool:
        duration = self.broken_duration(now=now)
        if duration is None:
            return False
        return duration >= self.broken_restart_seconds

    def oldest_broken_farm(self) -> str | None:
        if not self.broken_since_by_farm:
            return None
        return min(
            self.broken_since_by_farm,
            key=self.broken_since_by_farm.__getitem__,
        )

    def market_data_ready(self) -> bool:
        """True when no tracked farm is broken/connecting; cold start is ready."""

        if not self.farms and self.status is FarmLinkStatus.UNKNOWN:
            return True
        if self.status in {FarmLinkStatus.BROKEN, FarmLinkStatus.CONNECTING}:
            return False
        return not any(
            status in {FarmLinkStatus.BROKEN, FarmLinkStatus.CONNECTING}
            for status in self.farms.values()
        )

    def reset(self) -> None:
        self.status = FarmLinkStatus.UNKNOWN
        self.broken_since = None
        self.last_error_code = None
        self.last_error_message = None
        self.last_farm = None
        self.farms.clear()
        self.broken_since_by_farm.clear()


def request_gateway_restart(
    *,
    service: str = "ibc-gateway.service",
    scope: str = "--user",
) -> bool:
    result = subprocess.run(
        ["systemctl", scope, "restart", service],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe IBKR Gateway data-plane health.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON and use exit code 0=healthy, 1=unhealthy.",
    )
    return parser.parse_args(argv)


def run_probe_cli(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = IbkrSettings.from_env()
    runtime_policy = RuntimePolicySettings.from_env()

    if runtime_blocks_gateway_restart(runtime_policy):
        payload = {"ok": True, "skipped": "protected_mode"}
        if args.json:
            print(json.dumps(payload, sort_keys=True))
        return 0

    try:
        from ib_async import IB
    except ImportError:
        print("Missing dependency: ib_async", file=sys.stderr)
        return 2

    from spx_spark.ibkr.verifier import connect_market_data_only, prepare_ib_client

    ib = IB()
    prepare_ib_client(ib, request_timeout_seconds=settings.request_timeout_seconds)
    probe_client_id = settings.client_id + 900
    probe_settings = IbkrSettings(**{**asdict(settings), "client_id": probe_client_id})
    try:
        connect_market_data_only(ib, probe_settings)
        result = probe_data_plane(ib, settings)
    except Exception as exc:  # noqa: BLE001
        result = DataPlaneProbeResult(
            ok=False,
            current_time_ok=False,
            qualify_ok=False,
            error=f"connect failed: {exc}",
        )
    finally:
        if ib.isConnected():
            ib.disconnect()

    payload = {
        "ok": result.ok,
        "current_time_ok": result.current_time_ok,
        "qualify_ok": result.qualify_ok,
        "error": result.error,
        "current_time": result.current_time,
    }
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    elif not result.ok:
        print(result.error or "data plane unhealthy", file=sys.stderr)
    return 0 if result.ok else 1


def main() -> None:
    raise SystemExit(run_probe_cli())


if __name__ == "__main__":
    main()
