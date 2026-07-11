"""Deterministic, opaque identifiers for retry-safe storage writes."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import PurePath
from typing import Mapping


ID_SCHEMA_VERSION = 1
_DOMAIN = b"spx-spark-data-platform-id-v1\0"
_KIND_RE = re.compile(r"[a-z0-9_]+")


def _canonical(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("identifier timestamps must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, PurePath):
        return value.as_posix()
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("identifier mapping keys must be strings")
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"unsupported identifier component: {type(value).__name__}")


def deterministic_id(kind: str, *components: object) -> str:
    """Return a stable namespaced ID for canonical business-key components."""

    normalized_kind = kind.strip().lower().replace("-", "_")
    if not _KIND_RE.fullmatch(normalized_kind):
        raise ValueError("identifier kind must be alphanumeric with optional underscores")
    payload = json.dumps(
        [_canonical(component) for component in components],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256(_DOMAIN + normalized_kind.encode("ascii") + b"\0" + payload).hexdigest()
    return f"{normalized_kind}_{digest[:32]}"


def make_event_key(
    event_type: str,
    source_at: datetime,
    *source_identity: str,
) -> str:
    return deterministic_id("evt", event_type, source_at, source_identity)


def make_feature_snapshot_id(event_key: str, available_at: datetime, schema_version: int) -> str:
    return deterministic_id("feat", event_key, available_at, schema_version)


def make_decision_id(
    event_key: str | None,
    strategy_name: str,
    strategy_version: str,
    decision_at: datetime,
) -> str:
    return deterministic_id("dec", event_key, strategy_name, strategy_version, decision_at)


def make_delivery_id(decision_id: str, channel: str, attempted_at: datetime) -> str:
    return deterministic_id("delivery", decision_id, channel, attempted_at)


def make_outcome_id(event_key: str, decision_id: str | None, horizon_minutes: int) -> str:
    return deterministic_id("outcome", event_key, decision_id, horizon_minutes)


def make_compaction_manifest_id(source_path: str, source_sha256: str) -> str:
    return deterministic_id("compact", source_path, source_sha256)
