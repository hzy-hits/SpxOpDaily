from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IvSurfaceSettings, StorageSettings
from spx_spark.options_map import ExpiryOptionsMap, build_options_map
from spx_spark.storage import LatestState, LatestStateStore


@dataclass(frozen=True)
class IvSurfaceExpiry:
    expiry: str
    atm_iv: float | None
    atm_straddle_mid: float | None
    expected_move_points: float | None
    expected_move_pct: float | None
    put_skew_ratio: float | None
    call_skew_ratio: float | None
    smile_slope: float | None
    smile_curvature: float | None
    iv_surface_level: float | None
    iv_surface_shift_5m: float | None
    atm_iv_jump_5m: float | None
    put_skew_steepening_5m: float | None
    call_wing_bid: bool
    smile_curvature_change_5m: float | None
    surface_fit_quality: str
    wide_quote_surface_degraded: bool
    gamma_state: str
    zero_gamma: float | None
    put_wall: float | None
    call_wall: float | None
    option_count: int
    iv_coverage_ratio: float
    gamma_coverage_ratio: float
    avg_spread_bps: float | None
    warnings: tuple[str, ...]
    put_skew_25d: float | None = None
    put_skew_25d_change_5m: float | None = None


@dataclass(frozen=True)
class IvSurfaceSnapshot:
    created_at: datetime
    as_of: datetime
    underlier_price: float | None
    underlier_source: str | None
    front_expiry: str | None
    next_expiry: str | None
    front_vs_next_atm_iv_gap: float | None
    expiries: tuple[IvSurfaceExpiry, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        payload["as_of"] = self.as_of.isoformat()
        return payload


def average_present(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def subtract(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return current - previous


def previous_by_expiry(previous: IvSurfaceSnapshot | None) -> dict[str, IvSurfaceExpiry]:
    if previous is None:
        return {}
    return {item.expiry: item for item in previous.expiries}


def expiry_surface_level(expiry_map: ExpiryOptionsMap) -> float | None:
    return average_present(expiry_map.atm_iv, expiry_map.put_wing_iv, expiry_map.call_wing_iv)


def surface_fit_quality(expiry_map: ExpiryOptionsMap, *, wide_quote: bool) -> str:
    if expiry_map.option_count <= 0:
        return "missing_options"
    if expiry_map.atm_iv is None:
        return "missing_atm_iv"
    if expiry_map.coverage.total > 0 and expiry_map.coverage.with_iv / expiry_map.coverage.total < 0.5:
        return "low_iv_coverage"
    if wide_quote:
        return "wide_quote_degraded"
    return "raw_grid"


def build_expiry_surface(
    expiry_map: ExpiryOptionsMap,
    *,
    previous: IvSurfaceExpiry | None,
    wide_quote_spread_bps: float,
) -> IvSurfaceExpiry:
    smile_slope = subtract(expiry_map.call_wing_iv, expiry_map.put_wing_iv)
    wing_avg = average_present(expiry_map.put_wing_iv, expiry_map.call_wing_iv)
    smile_curvature = subtract(wing_avg, expiry_map.atm_iv)
    surface_level = expiry_surface_level(expiry_map)
    wide_quote = (
        expiry_map.coverage.avg_spread_bps is not None
        and expiry_map.coverage.avg_spread_bps > wide_quote_spread_bps
    )
    total = max(expiry_map.coverage.total, 1)
    return IvSurfaceExpiry(
        expiry=expiry_map.expiry,
        atm_iv=expiry_map.atm_iv,
        atm_straddle_mid=expiry_map.atm_straddle_mid,
        expected_move_points=expiry_map.expected_move_points,
        expected_move_pct=expiry_map.expected_move_pct,
        put_skew_ratio=expiry_map.put_skew_ratio,
        call_skew_ratio=expiry_map.call_skew_ratio,
        smile_slope=smile_slope,
        smile_curvature=smile_curvature,
        iv_surface_level=surface_level,
        iv_surface_shift_5m=subtract(surface_level, previous.iv_surface_level if previous else None),
        atm_iv_jump_5m=subtract(expiry_map.atm_iv, previous.atm_iv if previous else None),
        put_skew_steepening_5m=subtract(
            expiry_map.put_skew_ratio,
            previous.put_skew_ratio if previous else None,
        ),
        call_wing_bid=bool(expiry_map.call_skew_ratio is not None and expiry_map.call_skew_ratio >= 1.05),
        smile_curvature_change_5m=subtract(
            smile_curvature,
            previous.smile_curvature if previous else None,
        ),
        surface_fit_quality=surface_fit_quality(expiry_map, wide_quote=wide_quote),
        wide_quote_surface_degraded=wide_quote,
        gamma_state=expiry_map.gamma_state,
        zero_gamma=expiry_map.zero_gamma,
        put_wall=expiry_map.put_wall,
        call_wall=expiry_map.call_wall,
        option_count=expiry_map.option_count,
        iv_coverage_ratio=expiry_map.coverage.with_iv / total,
        gamma_coverage_ratio=expiry_map.coverage.with_gamma / total,
        avg_spread_bps=expiry_map.coverage.avg_spread_bps,
        warnings=expiry_map.warnings,
        put_skew_25d=expiry_map.put_skew_25d,
        put_skew_25d_change_5m=subtract(
            expiry_map.put_skew_25d,
            previous.put_skew_25d if previous else None,
        ),
    )


def build_iv_surface_snapshot(
    state: LatestState,
    *,
    settings: IvSurfaceSettings,
    previous: IvSurfaceSnapshot | None = None,
) -> IvSurfaceSnapshot:
    if previous is not None:
        gap_seconds = (state.as_of - previous.as_of).total_seconds()
        if gap_seconds > settings.diff_max_gap_seconds:
            previous = None
    options_map = build_options_map(state)
    previous_expiries = previous_by_expiry(previous)
    expiries = tuple(
        build_expiry_surface(
            expiry_map,
            previous=previous_expiries.get(expiry_map.expiry),
            wide_quote_spread_bps=settings.wide_quote_spread_bps,
        )
        for expiry_map in options_map.expiries
    )
    front = expiries[0] if len(expiries) >= 1 else None
    next_expiry = expiries[1] if len(expiries) >= 2 else None
    return IvSurfaceSnapshot(
        created_at=datetime.now(tz=timezone.utc),
        as_of=state.as_of,
        underlier_price=options_map.underlier.price,
        underlier_source=options_map.underlier.source,
        front_expiry=front.expiry if front else None,
        next_expiry=next_expiry.expiry if next_expiry else None,
        front_vs_next_atm_iv_gap=subtract(
            front.atm_iv if front else None,
            next_expiry.atm_iv if next_expiry else None,
        ),
        expiries=expiries,
        warnings=options_map.warnings,
    )


def snapshot_from_dict(payload: dict[str, Any]) -> IvSurfaceSnapshot:
    expiries = tuple(
        IvSurfaceExpiry(
            expiry=str(item.get("expiry") or ""),
            atm_iv=item.get("atm_iv"),
            atm_straddle_mid=item.get("atm_straddle_mid"),
            expected_move_points=item.get("expected_move_points"),
            expected_move_pct=item.get("expected_move_pct"),
            put_skew_ratio=item.get("put_skew_ratio"),
            call_skew_ratio=item.get("call_skew_ratio"),
            smile_slope=item.get("smile_slope"),
            smile_curvature=item.get("smile_curvature"),
            iv_surface_level=item.get("iv_surface_level"),
            iv_surface_shift_5m=item.get("iv_surface_shift_5m"),
            atm_iv_jump_5m=item.get("atm_iv_jump_5m"),
            put_skew_steepening_5m=item.get("put_skew_steepening_5m"),
            call_wing_bid=bool(item.get("call_wing_bid")),
            smile_curvature_change_5m=item.get("smile_curvature_change_5m"),
            surface_fit_quality=str(item.get("surface_fit_quality") or "unknown"),
            wide_quote_surface_degraded=bool(item.get("wide_quote_surface_degraded")),
            gamma_state=str(item.get("gamma_state") or "unknown"),
            zero_gamma=item.get("zero_gamma"),
            put_wall=item.get("put_wall"),
            call_wall=item.get("call_wall"),
            option_count=int(item.get("option_count") or 0),
            iv_coverage_ratio=float(item.get("iv_coverage_ratio") or 0.0),
            gamma_coverage_ratio=float(item.get("gamma_coverage_ratio") or 0.0),
            avg_spread_bps=item.get("avg_spread_bps"),
            warnings=tuple(item.get("warnings") or ()),
            put_skew_25d=item.get("put_skew_25d"),
            put_skew_25d_change_5m=item.get("put_skew_25d_change_5m"),
        )
        for item in payload.get("expiries", ())
        if isinstance(item, dict)
    )
    return IvSurfaceSnapshot(
        created_at=datetime.fromisoformat(payload["created_at"]),
        as_of=datetime.fromisoformat(payload["as_of"]),
        underlier_price=payload.get("underlier_price"),
        underlier_source=payload.get("underlier_source"),
        front_expiry=payload.get("front_expiry"),
        next_expiry=payload.get("next_expiry"),
        front_vs_next_atm_iv_gap=payload.get("front_vs_next_atm_iv_gap"),
        expiries=expiries,
        warnings=tuple(payload.get("warnings") or ()),
    )


def load_latest_snapshot(path: str | Path) -> IvSurfaceSnapshot | None:
    latest_path = Path(path)
    if not latest_path.exists():
        return None
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return snapshot_from_dict(payload)


def raw_snapshot_path(settings: IvSurfaceSettings, snapshot: IvSurfaceSnapshot) -> Path:
    timestamp = snapshot.as_of.astimezone(timezone.utc)
    return raw_snapshot_path_for_hour(settings, timestamp)


def raw_snapshot_path_for_hour(settings: IvSurfaceSettings, timestamp: datetime) -> Path:
    timestamp = timestamp.astimezone(timezone.utc)
    return (
        Path(settings.data_root)
        / "features"
        / "iv_surface"
        / f"date={timestamp.strftime('%Y-%m-%d')}"
        / f"hour={timestamp.strftime('%H')}"
        / settings.raw_file_name
    )


def raw_snapshot_paths_for_window(
    settings: IvSurfaceSettings,
    *,
    start: datetime,
    end: datetime,
) -> list[Path]:
    start_utc = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = end.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    paths: list[Path] = []
    current = start_utc
    while current <= end_utc:
        paths.append(raw_snapshot_path_for_hour(settings, current))
        current += timedelta(hours=1)
    return paths


def load_recent_snapshots(
    settings: IvSurfaceSettings,
    *,
    as_of: datetime,
    lookback_minutes: int = 60,
) -> list[IvSurfaceSnapshot]:
    end = as_of.astimezone(timezone.utc)
    start = end - timedelta(minutes=lookback_minutes)
    snapshots: list[IvSurfaceSnapshot] = []
    for path in raw_snapshot_paths_for_window(settings, start=start, end=end):
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                snapshot = snapshot_from_dict(json.loads(line))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            snapshot_as_of = snapshot.as_of.astimezone(timezone.utc)
            if start <= snapshot_as_of <= end:
                snapshots.append(snapshot)
    return sorted(snapshots, key=lambda item: item.as_of)


def summarize_surface_history(
    current: IvSurfaceSnapshot | None,
    history: list[IvSurfaceSnapshot],
) -> dict[str, Any] | None:
    if current is None:
        return None
    snapshots = list(history)
    if all(item.as_of != current.as_of for item in snapshots):
        snapshots.append(current)
    snapshots = sorted(snapshots, key=lambda item: item.as_of)
    first_by_expiry: dict[str, IvSurfaceExpiry] = {}
    for snapshot in snapshots:
        for expiry in snapshot.expiries:
            first_by_expiry.setdefault(expiry.expiry, expiry)

    expiry_rows = []
    for expiry in current.expiries[:2]:
        first = first_by_expiry.get(expiry.expiry)
        expiry_rows.append(
            {
                "expiry": expiry.expiry,
                "atm_iv_change_1h": subtract(expiry.atm_iv, first.atm_iv if first else None),
                "iv_surface_level_change_1h": subtract(
                    expiry.iv_surface_level,
                    first.iv_surface_level if first else None,
                ),
                "put_skew_change_1h": subtract(
                    expiry.put_skew_ratio,
                    first.put_skew_ratio if first else None,
                ),
                "call_skew_change_1h": subtract(
                    expiry.call_skew_ratio,
                    first.call_skew_ratio if first else None,
                ),
                "smile_curvature_change_1h": subtract(
                    expiry.smile_curvature,
                    first.smile_curvature if first else None,
                ),
                "surface_fit_quality": expiry.surface_fit_quality,
            }
        )
    return {
        "lookback_minutes": 60,
        "snapshot_count": len(snapshots),
        "start_as_of": snapshots[0].as_of.isoformat() if snapshots else None,
        "end_as_of": snapshots[-1].as_of.isoformat() if snapshots else None,
        "expiries": expiry_rows,
    }


def write_snapshot(settings: IvSurfaceSettings, snapshot: IvSurfaceSnapshot) -> dict[str, str]:
    payload = snapshot.to_dict()
    raw_path = raw_snapshot_path(settings, snapshot)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with raw_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        handle.write("\n")
    latest_path = Path(settings.latest_surface_path)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = latest_path.with_suffix(f"{latest_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(latest_path)
    return {"raw_path": str(raw_path), "latest_path": str(latest_path)}


def print_snapshot(snapshot: IvSurfaceSnapshot, paths: dict[str, str] | None = None) -> None:
    print(f"IV surface as of: {snapshot.as_of.isoformat()}")
    print(f"Underlier: {snapshot.underlier_price or '-'} source={snapshot.underlier_source or '-'}")
    print(f"0DTE vs next ATM IV gap: {snapshot.front_vs_next_atm_iv_gap}")
    if snapshot.warnings:
        print("Warnings:")
        for warning in snapshot.warnings:
            print(f"- {warning}")
    for item in snapshot.expiries:
        print(
            f"- {item.expiry}: quality={item.surface_fit_quality} "
            f"atm_iv={item.atm_iv} put_skew={item.put_skew_ratio} "
            f"call_skew={item.call_skew_ratio} curvature={item.smile_curvature}"
        )
    if paths:
        print(f"Raw: {paths['raw_path']}")
        print(f"Latest: {paths['latest_path']}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and persist a 5-minute IV surface snapshot.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    parser.add_argument("--no-write", action="store_true", help="Do not persist the snapshot.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = IvSurfaceSettings.from_env()
    state = LatestStateStore(StorageSettings.from_env()).load()
    previous = load_latest_snapshot(settings.latest_surface_path)
    snapshot = build_iv_surface_snapshot(state, settings=settings, previous=previous)
    paths = None if args.no_write else write_snapshot(settings, snapshot)
    if args.json:
        payload = snapshot.to_dict()
        if paths:
            payload["paths"] = paths
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_snapshot(snapshot, paths)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
