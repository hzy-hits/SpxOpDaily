"""Shared read contract for durable official-SPX minute samples."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from spx_spark.config import StorageSettings


def canonical_spx_minute_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "spx_standardized_minutes.json"


def load_standardized_spx_samples(data_root: str | Path) -> list[dict[str, object]]:
    path = Path(data_root) / "latest" / "spx_standardized_minutes.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("rows") if isinstance(payload, Mapping) else []
    return [
        {
            "at": row.get("minute"),
            "session_id": row.get("session_date"),
            "segment": "rth",
            "instruments": (
                {"index:SPX": dict(row["selected"])}
                if isinstance(row, Mapping) and isinstance(row.get("selected"), Mapping)
                else {}
            ),
            "spx_sampling": {
                "status": row.get("status"),
                "drop_reasons": row.get("drop_reasons"),
                "snapshot_generation": row.get("snapshot_generation"),
            },
        }
        for row in rows
        if isinstance(row, Mapping) and row.get("official_spx_expected") is True
    ]
