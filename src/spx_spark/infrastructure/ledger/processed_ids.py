"""Durable processed-event-id set for outbox consumer idempotency."""

from __future__ import annotations

import json
import os
from pathlib import Path

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

    def _load(self) -> set[str]:
        if not self.path.is_file():
            return set()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        if isinstance(payload, dict):
            raw = payload.get("event_ids", [])
        elif isinstance(payload, list):
            raw = payload
        else:
            return set()
        return {str(item) for item in raw if str(item).strip()}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Bound growth: keep the most recently added ids by sorting (stable enough).
        ordered = sorted(self._ids)
        if len(ordered) > self.max_ids:
            ordered = ordered[-self.max_ids :]
            self._ids = set(ordered)
        atomic_write_json_secure(
            self.path,
            {"schema_version": 1, "event_ids": ordered},
        )
        os.chmod(self.path, 0o600)

    def __contains__(self, event_id: object) -> bool:
        return str(event_id) in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def add(self, event_id: str) -> None:
        key = str(event_id)
        if key in self._ids:
            return
        self._ids.add(key)
        self._save()

    def as_set(self) -> set[str]:
        return set(self._ids)
