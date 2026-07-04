from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from spx_spark.config import RuntimePolicySettings


VALID_MODES = {"auto", "protected", "ibkr_on", "ibkr_off"}


@dataclass(frozen=True)
class RuntimeModeOverride:
    mode: str
    reason: str
    created_at: datetime
    expires_at: datetime | None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "RuntimeModeOverride":
        mode = str(raw.get("mode", "")).replace("-", "_")
        if mode not in VALID_MODES:
            raise ValueError(f"Unsupported runtime mode: {mode!r}")
        return cls(
            mode=mode,
            reason=str(raw.get("reason", "")),
            created_at=parse_datetime(str(raw["created_at"])),
            expires_at=parse_datetime(str(raw["expires_at"])) if raw.get("expires_at") else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "reason": self.reason,
            "created_at": format_datetime(self.created_at),
            "expires_at": format_datetime(self.expires_at) if self.expires_at else None,
        }

    def is_expired(self, now: datetime | None = None) -> bool:
        if self.expires_at is None:
            return False
        if now is None:
            now = datetime.now(tz=UTC)
        return now.astimezone(UTC) >= self.expires_at.astimezone(UTC)


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def load_override(path: str | os.PathLike[str], now: datetime | None = None) -> RuntimeModeOverride | None:
    override_path = Path(path)
    if not override_path.exists():
        return None
    with override_path.open(encoding="utf-8") as handle:
        override = RuntimeModeOverride.from_dict(json.load(handle))
    if override.is_expired(now):
        return None
    return override


def write_override(
    path: str | os.PathLike[str],
    mode: str,
    *,
    ttl_minutes: int | None,
    reason: str,
    now: datetime | None = None,
) -> RuntimeModeOverride:
    normalized_mode = mode.replace("-", "_")
    if normalized_mode not in VALID_MODES:
        raise ValueError(f"Unsupported runtime mode: {mode!r}")
    if now is None:
        now = datetime.now(tz=UTC)
    expires_at = now + timedelta(minutes=ttl_minutes) if ttl_minutes else None
    override = RuntimeModeOverride(
        mode=normalized_mode,
        reason=reason,
        created_at=now,
        expires_at=expires_at,
    )
    override_path = Path(path)
    override_path.parent.mkdir(parents=True, exist_ok=True)
    with override_path.open("w", encoding="utf-8") as handle:
        json.dump(override.to_dict(), handle, indent=2, sort_keys=True)
        handle.write("\n")
    return override


def clear_override(path: str | os.PathLike[str]) -> None:
    override_path = Path(path)
    if override_path.exists():
        override_path.unlink()


def ibkr_allowed(
    policy: RuntimePolicySettings,
    *,
    now: datetime | None = None,
    override: RuntimeModeOverride | None = None,
) -> bool:
    if override is None:
        return policy.market_data_collection_allowed(now)
    if override.mode == "auto":
        return policy.market_data_collection_allowed(now)
    if override.mode == "ibkr_on":
        return True
    return False


def describe_policy(policy: RuntimePolicySettings, override: RuntimeModeOverride | None) -> str:
    allowed = ibkr_allowed(policy, override=override)
    lines = [
        f"IBKR allowed now: {str(allowed).lower()}",
        (
            "Schedule: "
            f"{policy.ibkr_schedule_start.strftime('%H:%M')}-"
            f"{policy.ibkr_schedule_stop.strftime('%H:%M')} "
            f"{policy.ibkr_schedule_timezone}"
        ),
        "Schedule note: same start/stop means 24h eligible",
        f"Weekend maintenance mode: {str(policy.weekend_maintenance_mode).lower()}",
        f"Fallback provider: {policy.ibkr_fallback_provider}",
        f"Strict no session fight: {str(policy.strict_no_session_fight).lower()}",
        f"Conflict probe seconds: {policy.ibkr_conflict_probe_seconds}",
    ]
    if override is None:
        lines.append("Override: none")
    else:
        lines.append(f"Override: {override.mode}")
        if override.expires_at:
            lines.append(f"Override expires: {format_datetime(override.expires_at)}")
        if override.reason:
            lines.append(f"Override reason: {override.reason}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    policy = RuntimePolicySettings.from_env()
    parser = argparse.ArgumentParser(description="Manage SPX Spark runtime mode.")
    parser.add_argument(
        "mode",
        choices=["status", "auto", "protected", "ibkr-on", "ibkr-off", "clear"],
        help="Runtime mode to inspect or write.",
    )
    parser.add_argument("--path", default=policy.runtime_mode_path)
    parser.add_argument("--ttl-minutes", type=int)
    parser.add_argument("--reason", default="agent override")
    args = parser.parse_args(argv)

    if args.mode == "status":
        print(describe_policy(policy, load_override(args.path)))
        return
    if args.mode == "clear":
        clear_override(args.path)
        print(f"Cleared runtime override at {args.path}")
        return

    ttl_minutes = args.ttl_minutes
    if ttl_minutes is None and args.mode != "auto":
        ttl_minutes = policy.agent_override_default_ttl_minutes
    override = write_override(
        args.path,
        args.mode,
        ttl_minutes=ttl_minutes,
        reason=args.reason,
    )
    print(f"Wrote runtime override: {override.mode}")
    if override.expires_at:
        print(f"Expires: {format_datetime(override.expires_at)}")


if __name__ == "__main__":
    main()
