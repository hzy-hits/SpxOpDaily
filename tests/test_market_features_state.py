"""Unchanged-content write skipping for market feature state files."""

from __future__ import annotations

import json

from spx_spark.application.market_features.state import load_json, save_json


def test_save_json_skips_rewrite_when_content_unchanged(tmp_path) -> None:
    target = tmp_path / "market_feature_state.json"
    payload = {"schema_version": 1, "samples": [{"at": "2026-07-13", "price": 6300.5}]}

    save_json(target, payload)
    first_inode = target.stat().st_ino
    save_json(target, dict(payload))

    assert target.stat().st_ino == first_inode
    assert load_json(target) == payload
    assert not (tmp_path / "market_feature_state.json.tmp").exists()


def test_save_json_rewrites_when_content_changes(tmp_path) -> None:
    target = tmp_path / "market_feature_state.json"

    save_json(target, {"version": 1})
    first_inode = target.stat().st_ino
    save_json(target, {"version": 2})

    assert target.stat().st_ino != first_inode
    assert json.loads(target.read_text(encoding="utf-8")) == {"version": 2}
