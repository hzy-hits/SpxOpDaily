"""Append-only audit trail for LLM alert-review decisions.

The trail is deliberately compact and allowlisted. It records enough evidence
to explain a veto or a missed review without serializing prompts, environment
variables, API credentials, or notification endpoint URLs.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.policy import alert_key
from spx_spark.state_io import exclusive_state_lock


AUDIT_TEXT_MAX_CHARS = 8_000
AUDIT_ERROR_MAX_CHARS = 2_000
AUDIT_CANDIDATE_TEXT_MAX_CHARS = 1_000

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(api[_-]?key|authorization|bearer|password|secret|token|webhook)"
    r"\s*[:=]\s*([^\s,;]+)"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_URL_RE = re.compile(r"https?://[^\s)\]}]+", re.IGNORECASE)

_CANDIDATE_FIELDS = (
    "severity",
    "kind",
    "instrument_id",
    "title",
    "detail",
    "provider",
    "quality",
    "value",
    "threshold",
    "research_only",
    "source_gate",
    "dedup_group",
    "event_id",
)


def _redact_text(value: object, *, max_chars: int) -> str:
    text = str(value or "")
    text = _SECRET_ASSIGNMENT_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = _BEARER_RE.sub("Bearer <redacted>", text)
    text = _URL_RE.sub("<redacted-url>", text)
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "..."
    return text


def _safe_value(value: object) -> object:
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return _redact_text(value, max_chars=AUDIT_CANDIDATE_TEXT_MAX_CHARS)
    return _redact_text(value, max_chars=AUDIT_CANDIDATE_TEXT_MAX_CHARS)


def _safe_candidate(alert: dict[str, object]) -> dict[str, object]:
    candidate = {
        "alert_key": _redact_text(alert_key(alert), max_chars=AUDIT_CANDIDATE_TEXT_MAX_CHARS)
    }
    for field in _CANDIDATE_FIELDS:
        if field in alert:
            candidate[field] = _safe_value(alert.get(field))
    return candidate


def _safe_sink(sink: SinkResult) -> dict[str, object]:
    return {
        "sink": sink.sink,
        "attempted": sink.attempted,
        "ok": sink.ok,
        "dry_run": sink.dry_run,
        "exit_code": sink.exit_code,
        "error": _redact_text(sink.error, max_chars=AUDIT_ERROR_MAX_CHARS) if sink.error else None,
    }


def review_audit_path(settings: NotificationSettings) -> str:
    if settings.review_audit_path:
        return settings.review_audit_path
    state_path = Path(settings.state_path)
    return str(state_path.with_name("alert_review_audit.jsonl"))


def append_review_audit(
    settings: NotificationSettings,
    *,
    at: datetime,
    reviewer: str,
    candidates: list[dict[str, object]],
    raw_reply: str,
    parser_verdict: str,
    scope_ok: bool | None,
    outcome: str,
    reviewer_sink: SinkResult | None = None,
    delivery_sinks: list[SinkResult] | tuple[SinkResult, ...] = (),
    error: str | None = None,
    details: dict[str, object] | None = None,
) -> None:
    """Persist one complete reviewer outcome without ever blocking delivery."""

    path = review_audit_path(settings)
    if not path:
        return
    timestamp = at if at.tzinfo is not None else at.replace(tzinfo=timezone.utc)
    entry: dict[str, Any] = {
        "at": timestamp.astimezone(timezone.utc).isoformat(),
        "reviewer": reviewer,
        "candidate_count": len(candidates),
        "candidates": [_safe_candidate(alert) for alert in candidates],
        "raw_reply": _redact_text(raw_reply, max_chars=AUDIT_TEXT_MAX_CHARS),
        "parser_verdict": parser_verdict,
        "scope_ok": scope_ok,
        "outcome": outcome,
        "reviewer_sink": _safe_sink(reviewer_sink) if reviewer_sink is not None else None,
        "delivery_sinks": [_safe_sink(sink) for sink in delivery_sinks],
        "error": _redact_text(error, max_chars=AUDIT_ERROR_MAX_CHARS) if error else None,
    }
    if details:
        entry["details"] = {str(key): _safe_value(value) for key, value in details.items()}

    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = (json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        with exclusive_state_lock(target):
            descriptor = os.open(target, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                os.fchmod(descriptor, 0o600)
                os.write(descriptor, encoded)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except OSError:
        # Alert delivery must not fail merely because the audit disk is unavailable.
        return
