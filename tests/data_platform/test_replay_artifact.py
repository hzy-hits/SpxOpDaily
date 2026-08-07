from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.lake.layout import discover_raw_quote_partitions
from spx_spark.data_platform.lake.manifest import load_manifest
from spx_spark.data_platform.replay_artifact import (
    StoragePressure,
    cleanup_authorized_replay_sources,
    discover_partitions_for_window,
    prepare_replay_sources,
    publish_replay_artifact,
    verify_replay_artifact,
)


NOW = datetime(2026, 7, 10, 13, 10, tzinfo=timezone.utc)
TRADING_DATE = date(2026, 7, 10)


def quote_payload(
    *,
    received_at: str = "2026-07-10T10:05:00+00:00",
    provider: str = "ibkr",
    instrument_id: str = "option:SPX:SPXW:20260710:6200:C",
) -> dict[str, object]:
    return {
        "instrument": {
            "symbol": "SPX",
            "instrument_type": "option",
            "provider_symbol": "SPXW 260710C06200000",
            "exchange": "SMART",
            "currency": "USD",
            "expiry": "20260710",
            "strike": 6200.0,
            "right": "C",
            "multiplier": "100",
            "underlier": "SPX",
            "trading_class": "SPXW",
            "canonical_id": instrument_id,
        },
        "instrument_id": instrument_id,
        "provider": provider,
        "provider_symbol": "SPXW 260710C06200000",
        "received_at": received_at,
        "quality": "live",
        "bid": 10.0,
        "ask": 10.4,
        "last": 10.2,
        "mark": 10.2,
        "bid_size": 3.0,
        "ask_size": 4.0,
        "quote_time": received_at,
        "source_latency_ms": 0.0,
        "market_data_type": 1,
        "greeks": {
            "implied_vol": 0.2,
            "delta": 0.51,
            "gamma": 0.004,
            "theta": -1.2,
            "vega": 0.4,
            "rho": None,
            "underlier_price": 6201.0,
            "model": "ibkr",
        },
        "sampling_mode": "execution_monitor",
        "sampling_group": 0,
        "mid": 10.2,
        "spread": 0.4,
        "spread_bps": 392.1568627,
        "effective_price": 10.2,
    }


def source_path(data_root: Path, *, hour: int, provider: str = "ibkr") -> Path:
    return (
        data_root
        / "raw"
        / f"provider={provider}"
        / "date=2026-07-10"
        / f"hour={hour:02d}"
        / "quotes.jsonl"
    )


def write_source(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    settled = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(path, (settled, settled))


def prepare(tmp_path: Path):
    partitions = discover_raw_quote_partitions(tmp_path)
    preparation = prepare_replay_sources(
        partitions,
        compactor=QuoteLakeCompactor(tmp_path, settle_seconds=0, raw_delete_enabled=False),
        now=NOW,
    )
    assert preparation.ready, preparation.errors
    return preparation


def review_bytes(label: str = "one") -> tuple[bytes, bytes]:
    payload = {
        "created_at": "2026-07-10T21:00:00+00:00",
        "trading_date": TRADING_DATE.isoformat(),
        "label": label,
    }
    return (
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
        f"# SPX/SPXW Post-Close Review - {TRADING_DATE.isoformat()}\n\n{label}\n".encode(),
    )


def publish(tmp_path: Path, preparation, *, label: str = "one"):
    review_json, review_markdown = review_bytes(label)
    return publish_replay_artifact(
        tmp_path,
        trading_date=TRADING_DATE,
        window_start=datetime(2026, 7, 10, 9, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
        sources=preparation.sources,
        review_json=review_json,
        review_markdown=review_markdown,
        generated_at=NOW,
    )


def low_pressure() -> StoragePressure:
    return StoragePressure(
        total_bytes=100 * 1024**3,
        used_bytes=81 * 1024**3,
        free_bytes=19 * 1024**3,
        level="critical",
        action_required=True,
    )


def normal_pressure() -> StoragePressure:
    return StoragePressure(
        total_bytes=100 * 1024**3,
        used_bytes=60 * 1024**3,
        free_bytes=40 * 1024**3,
        level="normal",
        action_required=False,
    )


def test_discovers_every_utc_hour_intersecting_partial_window(tmp_path: Path) -> None:
    for hour in (8, 9, 10, 11, 12):
        write_source(source_path(tmp_path, hour=hour), [quote_payload()])

    partitions = discover_partitions_for_window(
        tmp_path,
        start=datetime(2026, 7, 10, 9, 15, tzinfo=timezone.utc),
        end=datetime(2026, 7, 10, 12, 15, tzinfo=timezone.utc),
    )

    assert [partition.hour for partition in partitions] == [9, 10, 11, 12]


def test_dry_run_reports_unprepared_sources_without_writing_lake(tmp_path: Path) -> None:
    raw = source_path(tmp_path, hour=10)
    write_source(raw, [quote_payload()])

    preparation = prepare_replay_sources(
        discover_raw_quote_partitions(tmp_path),
        compactor=QuoteLakeCompactor(tmp_path, settle_seconds=0, raw_delete_enabled=False),
        now=NOW,
        dry_run=True,
    )

    assert preparation.status == "would_prepare"
    assert preparation.results[0].status == "would_compact"
    assert not (tmp_path / "lake").exists()


def test_publish_is_atomic_immutable_and_idempotent_for_same_bytes(tmp_path: Path) -> None:
    write_source(source_path(tmp_path, hour=10), [quote_payload()])
    preparation = prepare(tmp_path)

    first = publish(tmp_path, preparation)
    second = publish(tmp_path, preparation)
    manifest_path = tmp_path / first.manifest_path
    manifest = verify_replay_artifact(manifest_path, data_root=tmp_path)

    assert first.status == "published"
    assert second.status == "already_published"
    assert first.revision == second.revision == manifest.revision
    assert manifest.revision != manifest.source_digest
    assert manifest.artifact_id == first.artifact_id
    assert manifest.generated_at == NOW.isoformat()
    assert manifest.review_json.sha256
    assert manifest.review_markdown.sha256
    assert (manifest_path.stat().st_mode & 0o777) == 0o600
    assert not list(manifest_path.parent.parent.glob(".*.tmp"))


def test_same_sources_with_changed_review_bytes_publish_a_new_revision(tmp_path: Path) -> None:
    write_source(source_path(tmp_path, hour=10), [quote_payload()])
    preparation = prepare(tmp_path)
    first = publish(tmp_path, preparation)
    second = publish(tmp_path, preparation, label="changed algorithm output")

    assert first.revision != second.revision
    assert first.artifact_id != second.artifact_id
    assert (tmp_path / first.manifest_path).is_file()
    assert (tmp_path / second.manifest_path).is_file()


def test_late_source_creates_new_revision_and_old_revision_does_not_authorize_it(
    tmp_path: Path,
) -> None:
    write_source(source_path(tmp_path, hour=10), [quote_payload()])
    first_preparation = prepare(tmp_path)
    first = publish(tmp_path, first_preparation)
    write_source(
        source_path(tmp_path, hour=11),
        [quote_payload(received_at="2026-07-10T11:05:00+00:00")],
    )

    blocked = cleanup_authorized_replay_sources(
        tmp_path,
        now=NOW + timedelta(hours=25),
        pressure=low_pressure(),
        grace_hours=24,
    )
    second_preparation = prepare(tmp_path)
    second = publish(tmp_path, second_preparation, label="late source included")
    old_manifest = verify_replay_artifact(tmp_path / first.manifest_path, data_root=tmp_path)
    new_manifest = verify_replay_artifact(tmp_path / second.manifest_path, data_root=tmp_path)

    assert blocked.status == "blocked"
    assert "does not authorize late/current" in blocked.blocked_dates[0][1]
    assert source_path(tmp_path, hour=10).exists()
    assert first.revision != second.revision
    assert len(old_manifest.sources) == 1
    assert len(new_manifest.sources) == 2


def test_cleanup_requires_pressure_and_both_24_hour_grace_clocks(tmp_path: Path) -> None:
    raw = source_path(tmp_path, hour=10)
    write_source(raw, [quote_payload()])
    preparation = prepare(tmp_path)
    publish(tmp_path, preparation)

    normal = cleanup_authorized_replay_sources(
        tmp_path,
        now=NOW + timedelta(hours=30),
        pressure=normal_pressure(),
    )
    early = cleanup_authorized_replay_sources(
        tmp_path,
        now=NOW + timedelta(hours=23),
        pressure=low_pressure(),
    )

    assert normal.status == "pressure_normal"
    assert early.status == "blocked"
    assert "grace_not_elapsed" in early.blocked_dates[0][1]
    assert raw.exists()
    assert normal.deleted_files == early.deleted_files == 0


def test_cleanup_dry_run_then_exact_delete_and_audit_resumes_as_verified(
    tmp_path: Path,
) -> None:
    raw = source_path(tmp_path, hour=10)
    write_source(raw, [quote_payload(), quote_payload(instrument_id="index:SPX")])
    preparation = prepare(tmp_path)
    artifact = publish(tmp_path, preparation)
    eligible_at = NOW + timedelta(hours=25)

    planned = cleanup_authorized_replay_sources(
        tmp_path,
        now=eligible_at,
        pressure=low_pressure(),
        dry_run=True,
    )
    deleted = cleanup_authorized_replay_sources(
        tmp_path,
        now=eligible_at,
        pressure=low_pressure(),
    )
    after = cleanup_authorized_replay_sources(
        tmp_path,
        now=eligible_at,
        pressure=low_pressure(),
    )

    assert planned.status == "would_delete"
    assert planned.would_delete_files == 1
    assert planned.would_delete_bytes == preparation.sources[0].source_size
    assert raw.exists() is False
    assert deleted.status == "deleted"
    assert deleted.deleted_files == 1
    assert deleted.deleted_bytes == preparation.sources[0].source_size
    assert after.status == "no_authorized_sources"
    verify_replay_artifact(tmp_path / artifact.manifest_path, data_root=tmp_path)


def test_any_artifact_validation_failure_blocks_whole_date_before_unlink(tmp_path: Path) -> None:
    first_raw = source_path(tmp_path, hour=10)
    second_raw = source_path(tmp_path, hour=11)
    write_source(first_raw, [quote_payload()])
    write_source(
        second_raw,
        [quote_payload(received_at="2026-07-10T11:05:00+00:00")],
    )
    preparation = prepare(tmp_path)
    artifact = publish(tmp_path, preparation)
    manifest = verify_replay_artifact(tmp_path / artifact.manifest_path, data_root=tmp_path)
    markdown_path = tmp_path / manifest.review_markdown.path
    markdown_path.write_bytes(markdown_path.read_bytes() + b"tampered\n")

    cleanup = cleanup_authorized_replay_sources(
        tmp_path,
        now=NOW + timedelta(hours=25),
        pressure=low_pressure(),
    )

    assert cleanup.status == "blocked"
    assert cleanup.deleted_files == 0
    assert first_raw.exists()
    assert second_raw.exists()


def test_late_append_between_artifact_check_and_delete_is_never_recompacted_or_deleted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    raw = source_path(tmp_path, hour=10)
    write_source(raw, [quote_payload()])
    preparation = prepare(tmp_path)
    publish(tmp_path, preparation)
    partition = discover_raw_quote_partitions(tmp_path)[0]
    original_manifest = load_manifest(partition.manifest_path)
    assert original_manifest is not None
    original_delete = QuoteLakeCompactor.delete_raw_if_manifest_matches
    injected = False

    def append_then_delete(self, target, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            with raw.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        quote_payload(
                            received_at="2026-07-10T10:06:00+00:00",
                            instrument_id="index:SPX",
                        ),
                        sort_keys=True,
                    )
                    + "\n"
                )
        return original_delete(self, target, **kwargs)

    monkeypatch.setattr(
        QuoteLakeCompactor,
        "delete_raw_if_manifest_matches",
        append_then_delete,
    )

    cleanup = cleanup_authorized_replay_sources(
        tmp_path,
        now=NOW + timedelta(hours=25),
        pressure=low_pressure(),
    )

    assert cleanup.status == "delete_failed"
    assert cleanup.results[0].status == "raw_delete_blocked"
    assert raw.exists()
    assert load_manifest(partition.manifest_path) == original_manifest
