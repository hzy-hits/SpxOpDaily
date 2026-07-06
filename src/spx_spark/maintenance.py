from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.config import MaintenanceSettings, StorageSettings
from spx_spark.marketdata import Provider
from spx_spark.storage import LatestStateStore


PROTECTED_DATA_SEGMENTS = frozenset({"latest", "runtime"})
@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int
    modified_at: str
    category: str
    prune_candidate: bool
    reason: str | None


@dataclass(frozen=True)
class DirectorySummary:
    path: str
    exists: bool
    file_count: int
    total_bytes: int
    prune_candidate_count: int
    prune_candidate_bytes: int


@dataclass(frozen=True)
class MaintenanceReport:
    created_at: str
    disk_total_bytes: int
    disk_used_bytes: int
    disk_free_bytes: int
    disk_used_pct: float
    data_budget_bytes: int
    data_bytes: int
    data_budget_used_pct: float
    action_level: str
    settings: dict[str, object]
    summaries: list[DirectorySummary]
    prune_candidates: list[FileEntry]


def human_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f}{unit}"
        amount /= 1024
    return f"{amount:.1f}TB"


@dataclass(frozen=True)
class PruneResult:
    created_at: str
    executed: bool
    deleted_files: int
    deleted_bytes: int
    removed_empty_dirs: int
    skipped_protected: int
    errors: list[str]
    deleted_paths: list[str]


def classify_path(path: Path) -> str:
    parts = set(path.parts)
    if "latest" in parts:
        return "latest"
    if "preserved" in parts or "alert_windows" in parts:
        return "alerts"
    if "raw" in parts:
        return "raw"
    if "context" in parts:
        return "context"
    if "features" in parts:
        if "interval=1s" in parts:
            return "feature_1s"
        if "interval=5s" in parts:
            return "feature_5s"
        return "features"
    if "alerts" in parts:
        return "alerts"
    if "reports" in parts:
        return "reports"
    if "trash" in parts:
        return "trash"
    return "logs" if "logs" in parts else "other"


def is_protected_path(path: Path, data_root: Path) -> bool:
    if path.name.endswith(".lock"):
        return True
    try:
        relative = path.relative_to(data_root)
    except ValueError:
        return False
    return any(segment in PROTECTED_DATA_SEGMENTS for segment in relative.parts)


def prune_reason(
    category: str,
    modified_at: datetime,
    now: datetime,
    settings: MaintenanceSettings,
) -> str | None:
    if category in {"latest", "other", "reports"}:
        return None
    age = now - modified_at
    if category in {"raw", "context"} and age > timedelta(days=settings.raw_retention_days):
        return f"{category} older than {settings.raw_retention_days} days"
    if category == "alerts" and age > timedelta(days=settings.alert_window_retention_days):
        return f"alerts older than {settings.alert_window_retention_days} days"
    if category == "feature_1s" and age > timedelta(days=settings.feature_1s_retention_days):
        return f"1s features older than {settings.feature_1s_retention_days} days"
    if category == "feature_5s" and age > timedelta(days=settings.feature_5s_retention_days):
        return f"5s features older than {settings.feature_5s_retention_days} days"
    if category == "logs" and age > timedelta(days=settings.log_retention_days):
        return f"logs older than {settings.log_retention_days} days"
    if category == "trash" and age > timedelta(days=settings.trash_retention_days):
        return f"trash older than {settings.trash_retention_days} days"
    if category == "features" and age > timedelta(days=settings.feature_5s_retention_days):
        return f"features older than {settings.feature_5s_retention_days} days"
    return None


def scan_directory(
    root: Path,
    *,
    now: datetime,
    settings: MaintenanceSettings,
) -> tuple[DirectorySummary, list[FileEntry]]:
    if not root.exists():
        return DirectorySummary(str(root), False, 0, 0, 0, 0), []

    entries: list[FileEntry] = []
    total_bytes = 0
    candidate_bytes = 0
    candidate_count = 0

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        category = classify_path(path)
        if is_protected_path(path, Path(settings.data_root)):
            reason = None
        else:
            reason = prune_reason(category, modified_at, now, settings)
        total_bytes += stat.st_size
        if reason:
            candidate_count += 1
            candidate_bytes += stat.st_size
        entries.append(
            FileEntry(
                path=str(path),
                size_bytes=stat.st_size,
                modified_at=modified_at.isoformat(),
                category=category,
                prune_candidate=reason is not None,
                reason=reason,
            )
        )

    summary = DirectorySummary(
        path=str(root),
        exists=True,
        file_count=len(entries),
        total_bytes=total_bytes,
        prune_candidate_count=candidate_count,
        prune_candidate_bytes=candidate_bytes,
    )
    return summary, entries


def action_level(used_pct: float, settings: MaintenanceSettings) -> str:
    if used_pct >= settings.critical_pct:
        return "critical_stop_raw"
    if used_pct >= settings.prune_pct:
        return "prune"
    if used_pct >= settings.degraded_pct:
        return "degraded"
    if used_pct >= settings.compact_pct:
        return "compact"
    if used_pct >= settings.warn_pct:
        return "warn"
    return "ok"


def build_report(settings: MaintenanceSettings, now: datetime | None = None) -> MaintenanceReport:
    if now is None:
        now = datetime.now(tz=timezone.utc)

    data_root = Path(settings.data_root)
    logs_root = Path(settings.logs_root)
    roots = [data_root]
    if logs_root != data_root:
        roots.append(logs_root)

    summaries: list[DirectorySummary] = []
    entries: list[FileEntry] = []
    for root in roots:
        summary, root_entries = scan_directory(root, now=now, settings=settings)
        summaries.append(summary)
        entries.extend(root_entries)

    disk_usage = shutil.disk_usage(data_root if data_root.exists() else Path("."))
    data_bytes = sum(summary.total_bytes for summary in summaries if summary.path == str(data_root))
    data_budget_bytes = int(settings.data_budget_gb * 1024**3)
    disk_used_pct = (disk_usage.used / disk_usage.total) * 100 if disk_usage.total else 0.0
    data_budget_used_pct = (data_bytes / data_budget_bytes) * 100 if data_budget_bytes else 0.0
    candidates = [entry for entry in entries if entry.prune_candidate]
    return MaintenanceReport(
        created_at=now.isoformat(),
        disk_total_bytes=disk_usage.total,
        disk_used_bytes=disk_usage.used,
        disk_free_bytes=disk_usage.free,
        disk_used_pct=round(disk_used_pct, 2),
        data_budget_bytes=data_budget_bytes,
        data_bytes=data_bytes,
        data_budget_used_pct=round(data_budget_used_pct, 2),
        action_level=action_level(disk_used_pct, settings),
        settings=asdict(settings),
        summaries=summaries,
        prune_candidates=candidates,
    )


def write_report(report: MaintenanceReport, output_root: str) -> Path:
    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"maintenance-dry-run-{timestamp}.json"
    output_path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_report(report: MaintenanceReport) -> None:
    print(f"Action level: {report.action_level}")
    print(
        "Disk: "
        f"{human_bytes(report.disk_used_bytes)} used / {human_bytes(report.disk_total_bytes)} "
        f"({report.disk_used_pct:.2f}%), free {human_bytes(report.disk_free_bytes)}"
    )
    print(
        "Project data budget: "
        f"{human_bytes(report.data_bytes)} / {human_bytes(report.data_budget_bytes)} "
        f"({report.data_budget_used_pct:.2f}%)"
    )
    print("\nDirectories:")
    for summary in report.summaries:
        print(
            f"- {summary.path}: exists={summary.exists} files={summary.file_count} "
            f"size={human_bytes(summary.total_bytes)} "
            f"prune_candidates={summary.prune_candidate_count} "
            f"prune_bytes={human_bytes(summary.prune_candidate_bytes)}"
        )
    total_prune = sum(entry.size_bytes for entry in report.prune_candidates)
    print(
        f"\nDry-run prune candidates: {len(report.prune_candidates)} files, "
        f"{human_bytes(total_prune)}"
    )
    for entry in report.prune_candidates[:20]:
        print(f"- {entry.path} {human_bytes(entry.size_bytes)}: {entry.reason}")
    if len(report.prune_candidates) > 20:
        print(f"... {len(report.prune_candidates) - 20} more candidates in JSON report")


def remove_empty_directories(roots: list[Path]) -> int:
    removed = 0
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in sorted((p for p in root.rglob("*") if p.is_dir()), reverse=True):
            if path in seen:
                continue
            seen.add(path)
            try:
                path.rmdir()
            except OSError:
                continue
            removed += 1
    return removed


def execute_prune(
    report: MaintenanceReport,
    settings: MaintenanceSettings,
    *,
    execute: bool,
) -> PruneResult:
    data_root = Path(settings.data_root)
    deleted_paths: list[str] = []
    deleted_bytes = 0
    skipped_protected = 0
    errors: list[str] = []

    for entry in report.prune_candidates:
        path = Path(entry.path)
        if is_protected_path(path, data_root):
            skipped_protected += 1
            continue
        if not execute:
            continue
        try:
            path.unlink()
            deleted_paths.append(entry.path)
            deleted_bytes += entry.size_bytes
        except OSError as exc:
            errors.append(f"{entry.path}: {exc}")

    removed_empty_dirs = 0
    if execute:
        roots = [data_root]
        logs_root = Path(settings.logs_root)
        if logs_root != data_root:
            roots.append(logs_root)
        removed_empty_dirs = remove_empty_directories(roots)

    return PruneResult(
        created_at=report.created_at,
        executed=execute,
        deleted_files=len(deleted_paths),
        deleted_bytes=deleted_bytes,
        removed_empty_dirs=removed_empty_dirs,
        skipped_protected=skipped_protected,
        errors=errors,
        deleted_paths=deleted_paths,
    )


def write_prune_result(result: PruneResult, output_root: str) -> Path:
    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = "executed" if result.executed else "dry-run"
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"maintenance-prune-{suffix}-{timestamp}.json"
    output_path.write_text(json.dumps(asdict(result), indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_prune_result(report: MaintenanceReport, result: PruneResult) -> None:
    mode = "EXECUTED" if result.executed else "DRY-RUN"
    print(f"Prune mode: {mode}")
    print(f"Action level: {report.action_level}")
    print(
        f"Candidates: {len(report.prune_candidates)} files, "
        f"{human_bytes(sum(entry.size_bytes for entry in report.prune_candidates))}"
    )
    if result.executed:
        print(
            f"Deleted: {result.deleted_files} files, "
            f"{human_bytes(result.deleted_bytes)}, "
            f"empty_dirs_removed={result.removed_empty_dirs}"
        )
    else:
        printable = sum(
            entry.size_bytes
            for entry in report.prune_candidates
            if not is_protected_path(Path(entry.path), Path(report.settings["data_root"]))
        )
        print(f"Would delete: {len(report.prune_candidates) - result.skipped_protected} files, {human_bytes(printable)}")
    if result.skipped_protected:
        print(f"Skipped protected: {result.skipped_protected}")
    if result.errors:
        print("Errors:")
        for error in result.errors:
            print(f"- {error}")
    for entry in report.prune_candidates[:20]:
        print(f"- {entry.path} {human_bytes(entry.size_bytes)}: {entry.reason}")
    if len(report.prune_candidates) > 20:
        print(f"... {len(report.prune_candidates) - 20} more candidates in JSON report")


def purge_latest_provider(provider_name: str, *, settings: MaintenanceSettings) -> dict[str, object]:
    provider = Provider(provider_name.strip().lower())
    storage_settings = StorageSettings(
        data_root=settings.data_root,
        latest_state_path=f"{settings.data_root.rstrip('/')}/latest/state.json",
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
    )
    result = LatestStateStore(storage_settings).purge_provider_quotes(provider)
    return {
        "provider": provider.value,
        "latest_state": result.path,
        "provider_quote_count": result.provider_quote_count,
        "best_quote_count": result.best_quote_count,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SPX Spark maintenance utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    dry_run = subparsers.add_parser("dry-run", help="Scan disk and report cleanup candidates.")
    dry_run.add_argument("--json", action="store_true", help="Print full JSON report to stdout.")
    dry_run.add_argument("--no-write", action="store_true", help="Do not write report to disk.")
    prune = subparsers.add_parser("prune", help="Delete files older than retention policy.")
    prune.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete prune candidates. Default is dry-run only.",
    )
    prune.add_argument("--json", action="store_true", help="Print prune JSON to stdout.")
    prune.add_argument("--no-write", action="store_true", help="Do not write prune report to disk.")
    purge_latest = subparsers.add_parser(
        "purge-latest-provider",
        help="Remove one provider's quotes from latest/state.json.",
    )
    purge_latest.add_argument(
        "--provider",
        required=True,
        help="Provider id to purge from latest state (e.g. mock, ibkr).",
    )
    purge_latest.add_argument("--json", action="store_true", help="Print JSON result to stdout.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = MaintenanceSettings.from_env()
    if args.command == "dry-run":
        report = build_report(settings)
        if args.json:
            print(json.dumps(asdict(report), indent=2, sort_keys=True))
        else:
            print_report(report)
        if not args.no_write:
            output_path = write_report(report, settings.output_root)
            print(f"\nWrote JSON report: {output_path}")
        return 0
    if args.command == "prune":
        report = build_report(settings)
        result = execute_prune(report, settings, execute=args.execute)
        payload = {
            "report": asdict(report),
            "result": asdict(result),
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print_prune_result(report, result)
        if not args.no_write:
            output_path = write_prune_result(result, settings.output_root)
            print(f"\nWrote JSON report: {output_path}")
        return 1 if result.errors else 0
    if args.command == "purge-latest-provider":
        payload = purge_latest_provider(args.provider, settings=settings)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(
                f"Purged provider={payload['provider']} from latest state "
                f"({payload['best_quote_count']} best quotes remain)."
            )
        return 0
    return 2


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
