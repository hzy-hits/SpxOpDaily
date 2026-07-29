"""Durable processed-event-id set for outbox consumer idempotency."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.state_io import atomic_write_json_secure


class DurableProcessedIdSet:
    """Set-like store persisted as JSON; ``add`` is crash-safe via atomic write.

    Implements the minimal surface used by ``IdempotentOutboxConsumer``:
    ``__contains__`` and ``add``.
    """

    def __init__(self, path: str | Path, *, max_ids: int = 50_000) -> None:
        if max_ids < 1:
            raise ValueError("max_ids must be >= 1")
        self.path = Path(path)
        self.max_ids = max_ids
        self._ids = self._load()

    def _load(self) -> dict[str, str | None]:
        if not self.path.is_file():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        observations: object = None
        if isinstance(payload, dict):
            observations = payload.get("observations")
            raw = payload.get("event_ids", [])
        elif isinstance(payload, list):
            raw = payload
        else:
            return {}

        ordered: dict[str, str | None] = {}
        source = observations if isinstance(observations, list) else raw
        if not isinstance(source, list):
            return {}
        for item in source:
            if isinstance(item, Mapping):
                key = str(item.get("event_id") or item.get("id") or "").strip()
                observed_at_value = item.get("observed_at")
                observed_at = (
                    str(observed_at_value) if observed_at_value is not None else None
                )
            else:
                key = str(item).strip()
                observed_at = None
            if key and key not in ordered:
                ordered[key] = observed_at
        return dict(list(ordered.items())[-self.max_ids :])

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered = list(self._ids.items())[-self.max_ids :]
        self._ids = dict(ordered)
        atomic_write_json_secure(
            self.path,
            {
                "schema_version": 2,
                "event_ids": [event_id for event_id, _observed_at in ordered],
                "observations": [
                    {"event_id": event_id, "observed_at": observed_at}
                    for event_id, observed_at in ordered
                ],
            },
        )
        os.chmod(self.path, 0o600)

    def __contains__(self, event_id: object) -> bool:
        return str(event_id) in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def add(self, event_id: str) -> None:
        key = str(event_id).strip()
        if not key:
            return
        if key in self._ids:
            return
        self._ids[key] = datetime.now(tz=timezone.utc).isoformat()
        self._save()

    def as_set(self) -> set[str]:
        return set(self._ids)
