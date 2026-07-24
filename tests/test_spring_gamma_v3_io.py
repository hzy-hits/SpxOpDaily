from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features.spring_gamma_v3_io import (
    SCHEMA,
    SpringGammaV3ShadowContractError,
    persist_spring_gamma_v3_shadow,
    spring_gamma_v3_prediction_due,
    validate_spring_gamma_v3_shadow,
)


def prediction(
    *,
    as_of: str = "2026-07-24T14:00:00+00:00",
    prediction_id: str = "prediction-1",
    session_id: str = "2026-07-24:rth",
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA,
        "status": "ready",
        "as_of": as_of,
        "session_id": session_id,
        "prediction_id": prediction_id,
        "input_fingerprint": f"input:{prediction_id}",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "predictions": [
            {
                "horizon_minutes": 15,
                "direction": "up",
                "probability": 0.62,
            }
        ],
    }


def read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def read_jsonl(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def test_validate_accepts_all_shadow_statuses() -> None:
    for status in ("ready", "abstain", "failed", "disabled"):
        payload = prediction()
        payload["status"] = status
        assert validate_spring_gamma_v3_shadow(payload)["status"] == status


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("schema_version", "spring_gamma_v3_shadow.v2"),
        ("status", "trade_ready"),
        ("as_of", "2026-07-24T14:00:00"),
        ("as_of", "not-a-clock"),
        ("session_id", ""),
        ("prediction_id", None),
        ("input_fingerprint", " "),
        ("direction_authority", "model"),
        ("action_authority", "production"),
        ("actionable", True),
        ("automatic_ordering", True),
    ),
)
def test_validate_fails_closed(field: str, value: object) -> None:
    payload = prediction()
    payload[field] = value
    with pytest.raises(SpringGammaV3ShadowContractError):
        validate_spring_gamma_v3_shadow(payload)


def test_validate_rejects_any_additional_authority() -> None:
    payload = prediction()
    payload["execution_authority"] = "broker"
    with pytest.raises(SpringGammaV3ShadowContractError):
        validate_spring_gamma_v3_shadow(payload)


@pytest.mark.parametrize(
    "nested",
    (
        {"wall_probability": {"action_authority": "production"}},
        {"wall_probability": {"actionable": True}},
        {"diagnostics": [{"automatic_ordering": True}]},
    ),
)
def test_validate_rejects_nested_authority(nested: dict[str, object]) -> None:
    payload = {**prediction(), **nested}
    with pytest.raises(SpringGammaV3ShadowContractError):
        validate_spring_gamma_v3_shadow(payload)


def test_persist_writes_expected_raw_and_latest_paths(tmp_path: Path) -> None:
    payload = prediction()
    result = persist_spring_gamma_v3_shadow(
        payload,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )

    raw = (
        tmp_path
        / "features"
        / "spring_gamma_v3"
        / "date=2026-07-24"
        / "predictions.jsonl"
    )
    latest = tmp_path / "latest" / "spring_gamma_v3_shadow.json"
    assert result == {
        "raw_path": str(raw),
        "latest_path": str(latest),
        "bucket_start": "2026-07-24T14:00:00+00:00",
        "appended": True,
        "latest_updated": True,
    }
    assert read_jsonl(raw) == [payload]
    assert read_json(latest) == payload


def test_same_bucket_does_not_append_but_new_as_of_updates_latest(
    tmp_path: Path,
) -> None:
    first = prediction(as_of="2026-07-24T14:01:00Z", prediction_id="first")
    newer = prediction(as_of="2026-07-24T14:14:59Z", prediction_id="newer")

    persist_spring_gamma_v3_shadow(
        first,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )
    result = persist_spring_gamma_v3_shadow(
        newer,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )

    raw = Path(str(result["raw_path"]))
    latest = Path(str(result["latest_path"]))
    assert result["appended"] is False
    assert result["latest_updated"] is True
    assert read_jsonl(raw) == [first]
    assert read_json(latest)["prediction_id"] == "newer"


def test_stale_input_appends_to_raw_but_cannot_replace_latest(tmp_path: Path) -> None:
    newest = prediction(as_of="2026-07-24T14:30:00Z", prediction_id="newest")
    stale = prediction(as_of="2026-07-24T13:45:00Z", prediction_id="stale")

    persist_spring_gamma_v3_shadow(
        newest,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )
    result = persist_spring_gamma_v3_shadow(
        stale,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )

    assert result["appended"] is True
    assert result["latest_updated"] is False
    assert read_json(Path(str(result["latest_path"])))["prediction_id"] == "newest"
    assert {row["prediction_id"] for row in read_jsonl(Path(str(result["raw_path"])))} == {
        "newest",
        "stale",
    }


def test_next_bucket_appends_and_updates_latest(tmp_path: Path) -> None:
    first = prediction(as_of="2026-07-24T14:14:59Z", prediction_id="first")
    second = prediction(as_of="2026-07-24T14:15:00Z", prediction_id="second")

    persist_spring_gamma_v3_shadow(
        first,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )
    result = persist_spring_gamma_v3_shadow(
        second,
        data_root=tmp_path,
        prediction_interval_seconds=900,
    )

    assert result["appended"] is True
    assert result["latest_updated"] is True
    assert len(read_jsonl(Path(str(result["raw_path"])))) == 2
    assert read_json(Path(str(result["latest_path"])))["prediction_id"] == "second"


def test_prediction_due_uses_session_and_interval_bucket() -> None:
    latest = prediction(as_of="2026-07-24T14:00:20Z")

    assert not spring_gamma_v3_prediction_due(
        latest,
        now=datetime(2026, 7, 24, 14, 0, 59, tzinfo=timezone.utc),
        session_id="2026-07-24:rth",
        prediction_interval_seconds=60,
    )
    assert spring_gamma_v3_prediction_due(
        latest,
        now=datetime(2026, 7, 24, 14, 1, tzinfo=timezone.utc),
        session_id="2026-07-24:rth",
        prediction_interval_seconds=60,
    )
    assert spring_gamma_v3_prediction_due(
        latest,
        now=datetime(2026, 7, 24, 14, 0, 30, tzinfo=timezone.utc),
        session_id="2026-07-25:gth",
        prediction_interval_seconds=60,
    )


def test_flock_serializes_same_bucket_writers(tmp_path: Path) -> None:
    payloads = [
        prediction(
            as_of=f"2026-07-24T14:{minute:02d}:00Z",
            prediction_id=f"prediction-{minute}",
        )
        for minute in range(1, 10)
    ]

    def write(payload: dict[str, object]) -> dict[str, object]:
        return persist_spring_gamma_v3_shadow(
            payload,
            data_root=tmp_path,
            prediction_interval_seconds=900,
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(write, payloads))

    raw = Path(str(results[0]["raw_path"]))
    latest = Path(str(results[0]["latest_path"]))
    assert sum(result["appended"] is True for result in results) == 1
    assert len(read_jsonl(raw)) == 1
    assert read_json(latest)["prediction_id"] == "prediction-9"


@pytest.mark.parametrize("interval", (0, -1, 1.5, True))
def test_persist_rejects_invalid_prediction_interval(
    tmp_path: Path,
    interval: object,
) -> None:
    with pytest.raises(ValueError):
        persist_spring_gamma_v3_shadow(
            prediction(),
            data_root=tmp_path,
            prediction_interval_seconds=interval,  # type: ignore[arg-type]
        )
