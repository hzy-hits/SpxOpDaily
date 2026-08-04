from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.application.order_map.desk_projection_export import build_desk_map_wire
from spx_spark.config import StorageSettings


GOLDEN_ROOT = Path(__file__).resolve().parents[1] / "contracts" / "golden" / "domain"
UTC = timezone.utc


def _read_golden(version: str, name: str) -> dict[str, object]:
    return json.loads((GOLDEN_ROOT / version / name).read_text(encoding="utf-8"))


def _storage(root: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(root),
        latest_state_path=str(root / "latest" / "state.json"),
        raw_file_name="raw.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=30,
        slow_index_stale_after_seconds=30,
        slow_index_labels=frozenset(),
    )


def _desk_payload() -> dict[str, object]:
    return {
        "expiry": "20260803",
        "underlier": {"price": 7512.0, "source": "index:SPX"},
        "es_last": 7521.0,
        "flip_zone": [7490.0, 7510.0],
        "level_decision": {
            "event_id": "level-event:shared-golden",
            "phase": "confirmed",
            "direction": "up",
            "thesis": "breakout",
            "level_kind": "flip_high",
            "level": 7510.0,
            "levels": {
                "put_wall": 7450.0,
                "flip_low": 7490.0,
                "flip_high": 7510.0,
                "call_wall": 7550.0,
            },
        },
        "warnings": [],
    }


def test_python_accepts_and_preserves_shared_research_context_golden(tmp_path: Path) -> None:
    research = _read_golden("v2", "research_context.json")
    research_path = tmp_path / "latest" / "experimental_research_signals.json"
    research_path.parent.mkdir(parents=True)
    research_path.write_text(json.dumps(research), encoding="utf-8")

    wire = build_desk_map_wire(
        _desk_payload(),
        [],
        now=datetime(2026, 8, 3, 19, 16, tzinfo=UTC),
        published_at=datetime(2026, 8, 3, 19, 16, tzinfo=UTC),
        trading_date="2026-08-03",
        storage=_storage(tmp_path),
    )

    assert wire["research_context_document_id"] == research["document_id"]
    assert wire["research_context"] == research
    assert wire["action_authority"] == "none"
    assert wire["automatic_ordering"] is False


def test_shared_desk_map_golden_matches_python_wire_surface(tmp_path: Path) -> None:
    golden = _read_golden("v1", "desk_map_projection.json")
    research = _read_golden("v2", "research_context.json")
    generated = build_desk_map_wire(
        _desk_payload(),
        [],
        now=datetime(2026, 8, 3, 19, 16, tzinfo=UTC),
        trading_date="2026-08-03",
        storage=_storage(tmp_path),
    )

    assert set(golden) == set(generated)
    assert set(golden["message"]) == set(generated["message"])
    assert golden["research_context"] == research
    assert golden["research_context_document_id"] == research["document_id"]
    assert golden["action_authority"] == "none"
    assert golden["automatic_ordering"] is False
