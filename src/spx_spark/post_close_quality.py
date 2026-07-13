"""Review completeness verdict projection."""

from __future__ import annotations

from typing import Any


def review_verdict(
    payload: dict[str, Any],
    *,
    checks: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    if checks is None:
        raw_checks = payload.get("completeness", {}).get("checks", [])
        failures = [item for item in raw_checks if not bool(item.get("passed"))]
        warnings = [f"{item.get('name')}: {item.get('reason')}" for item in failures]
        check_count = len(raw_checks)
    else:
        failures = [check for check in checks if not check.passed]
        warnings = [f"{check.name}: {check.reason}" for check in failures]
        check_count = len(checks)
    return {
        "status": "complete" if check_count > 0 and not warnings else "degraded",
        "warnings": warnings,
        "required_checks": check_count,
        "passed_checks": check_count - len(failures),
    }
