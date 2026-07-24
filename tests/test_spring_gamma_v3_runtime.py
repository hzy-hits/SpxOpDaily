from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.market_features import service
from spx_spark.settings.spring_gamma_v3 import SpringGammaV3Settings


NOW = datetime(2026, 7, 24, 14, 0, tzinfo=timezone.utc)


class Frame:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def to_dict(self) -> dict[str, object]:
        return dict(self.payload)


def _record() -> dict[str, object]:
    return {
        "schema_version": "spring_gamma_v3_shadow.v1",
        "model_version": "spring_gamma_v3_es_only_shadow.v1",
        "prediction_id": "spring-gamma-v3:2026-07-24:20260724:test",
        "input_fingerprint": "a" * 64,
        "as_of": NOW.isoformat(),
        "session_id": "2026-07-24",
        "session": "rth",
        "expiry": "20260724",
        "status": "ready",
        "mode": "shadow",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "calibration_status": "uncalibrated_shadow",
        "direction": {"decision": "up"},
        "regime": "trend_continuation",
        "opportunity": "trend_continuation",
        "abstain": False,
        "abstain_reasons": [],
    }


def _run(tmp_path, **kwargs):
    settings = kwargs.pop("settings", SpringGammaV3Settings())
    return service._process_spring_gamma_v3_shadow(
        storage=SimpleNamespace(data_root=str(tmp_path)),
        latest_state=SimpleNamespace(),
        options_map=Frame({"expiries": []}),
        market_frame=Frame({"session_id": "2026-07-24"}),
        option_frame=Frame({}),
        greek_reference={},
        exposure_map=Frame({}),
        level_decision={},
        now=NOW,
        settings=settings,
        **kwargs,
    )


def test_runtime_persists_one_isolated_shadow_bucket(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", lambda **_: _record())
    monkeypatch.setattr(service, "group_spxw_option_quotes", lambda *_, **__: {})
    monkeypatch.setattr(
        service,
        "build_wall_probability_tenor_shadow",
        lambda **_: {"status": "ready", "abstain_reasons": []},
    )

    result = _run(tmp_path)

    latest = json.loads(
        (tmp_path / "latest" / "spring_gamma_v3_shadow.json").read_text(
            encoding="utf-8"
        )
    )
    assert result["status"] == "ready"
    assert result["appended"] is True
    assert latest["prediction_id"].startswith(
        "spring-gamma-v3:2026-07-24:20260724:"
    )
    assert latest["direction_authority"] == "none"
    assert latest["actionable"] is False
    assert latest["wall_probability"]["status"] == "ready"
    assert latest["direction_input_fingerprint"] == "a" * 64


def test_runtime_failure_is_persisted_as_non_actionable_abstain(
    tmp_path, monkeypatch
) -> None:
    def fail(**_: object) -> dict[str, object]:
        raise RuntimeError("research broke")

    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", fail)

    result = _run(tmp_path)

    latest = json.loads(
        (tmp_path / "latest" / "spring_gamma_v3_shadow.json").read_text(
            encoding="utf-8"
        )
    )
    assert result["status"] == "failed"
    assert latest["status"] == "failed"
    assert latest["direction"]["decision"] == "abstain"
    assert latest["direction_authority"] == "none"
    assert latest["action_authority"] == "none"
    assert latest["automatic_ordering"] is False


def test_runtime_does_not_recompute_inside_existing_minute_bucket(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", lambda **_: _record())
    monkeypatch.setattr(service, "group_spxw_option_quotes", lambda *_, **__: {})
    monkeypatch.setattr(
        service,
        "build_wall_probability_tenor_shadow",
        lambda **_: {"status": "ready", "abstain_reasons": []},
    )
    first = _run(tmp_path)

    def should_not_run(**_: object) -> dict[str, object]:
        raise AssertionError("same bucket must reuse durable shadow")

    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", should_not_run)
    second = _run(tmp_path)

    assert first["evaluated"] is True
    assert second == {
        "evaluated": False,
        "status": "ready",
        "prediction_id": first["prediction_id"],
    }


def test_wall_shadow_can_only_downgrade_the_direction_shadow() -> None:
    combined = service._attach_wall_probability_shadow(
        _record(),
        {
            "status": "abstain",
            "abstain_reasons": ["front_structure_frozen"],
            "direction_authority": "none",
            "action_authority": "none",
        },
    )

    assert combined["status"] == "abstain"
    assert combined["direction"]["decision"] == "abstain"
    assert combined["opportunity"] == "abstain"
    assert combined["actionable"] is False
    assert combined["automatic_ordering"] is False
    assert combined["direction_authority"] == "none"
    assert combined["action_authority"] == "none"
    assert "wall_probability:front_structure_frozen" in combined["abstain_reasons"]
    assert combined["direction_input_fingerprint"] == "a" * 64
    assert combined["input_fingerprint"] != combined["direction_input_fingerprint"]


def test_runtime_converts_nested_authority_violation_to_persisted_failure(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", lambda **_: _record())
    monkeypatch.setattr(service, "group_spxw_option_quotes", lambda *_, **__: {})
    monkeypatch.setattr(
        service,
        "build_wall_probability_tenor_shadow",
        lambda **_: {
            "status": "ready",
            "abstain_reasons": [],
            "action_authority": "production",
        },
    )

    result = _run(tmp_path)
    latest = json.loads(
        (tmp_path / "latest" / "spring_gamma_v3_shadow.json").read_text(
            encoding="utf-8"
        )
    )

    assert result["status"] == "failed"
    assert latest["status"] == "failed"
    assert latest["direction"]["decision"] == "abstain"
    assert latest["action_authority"] == "none"
    assert latest["actionable"] is False


def test_runtime_does_not_reuse_future_cross_expiry_or_unsafe_latest(
    tmp_path, monkeypatch
) -> None:
    latest_path = tmp_path / "latest" / "spring_gamma_v3_shadow.json"
    latest_path.parent.mkdir(parents=True)
    recomputations = 0

    def build_direction(**_: object) -> dict[str, object]:
        nonlocal recomputations
        recomputations += 1
        record = _record()
        record["prediction_id"] = f"recomputed-{recomputations}"
        return record

    monkeypatch.setattr(service, "build_spring_gamma_v3_shadow", build_direction)
    monkeypatch.setattr(service, "group_spxw_option_quotes", lambda *_, **__: {})
    monkeypatch.setattr(
        service,
        "build_wall_probability_tenor_shadow",
        lambda **_: {"status": "ready", "abstain_reasons": []},
    )

    variants = (
        {**_record(), "as_of": (NOW + timedelta(seconds=1)).isoformat()},
        {**_record(), "expiry": "20260727"},
        {**_record(), "actionable": True},
    )
    for index, latest in enumerate(variants, start=1):
        latest["prediction_id"] = f"invalid-latest-{index}"
        latest_path.write_text(json.dumps(latest), encoding="utf-8")

        result = _run(tmp_path)

        assert result["evaluated"] is True
        assert result["prediction_id"] != f"invalid-latest-{index}"

    assert recomputations == len(variants)


def test_invalid_shadow_interval_cannot_break_the_production_loop(tmp_path) -> None:
    result = _run(
        tmp_path,
        settings=SimpleNamespace(prediction_interval_seconds=0),
    )
    latest = json.loads(
        (tmp_path / "latest" / "spring_gamma_v3_shadow.json").read_text(
            encoding="utf-8"
        )
    )

    assert result["status"] == "failed"
    assert latest["status"] == "failed"
    assert latest["direction_authority"] == "none"
    assert latest["action_authority"] == "none"
    assert latest["actionable"] is False
