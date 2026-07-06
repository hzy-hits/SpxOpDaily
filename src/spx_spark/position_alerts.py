from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.config import IbkrPositionSettings, env_bool, env_float
from spx_spark.ibkr.position_watcher import (
    PositionSnapshot,
    SpxwPosition,
    load_snapshot,
    position_state_path,
)
from spx_spark.options_map import OptionsMap
from spx_spark.storage import LatestState


def position_holdings_alerts(
    state: LatestState,
    *,
    options_map: OptionsMap | None,
    window: AlertWindow,
) -> list[Alert]:
    position_settings = IbkrPositionSettings.from_env()
    if not position_settings.enabled or not position_settings.snapshot_path:
        return []
    snapshot = load_snapshot(position_settings.snapshot_path)
    previous = load_position_alert_state()
    return evaluate_position_alerts(
        snapshot,
        previous=previous,
        state=state,
        options_map=options_map,
        window=window,
        persist_state=True,
    )


@dataclass(frozen=True)
class PositionAlertState:
    positions: dict[str, float]
    leg_pnl: dict[str, float]
    book_pnl: float | None
    updated_at: str | None = None

    @classmethod
    def from_snapshot(cls, snapshot: PositionSnapshot | None) -> PositionAlertState:
        if snapshot is None:
            return cls(positions={}, leg_pnl={}, book_pnl=None)
        return cls(
            positions={item.position_key: item.qty for item in snapshot.positions},
            leg_pnl={},
            book_pnl=snapshot.book_unrealized_pnl,
            updated_at=snapshot.fetched_at,
        )


def load_position_alert_state(path: str | None = None) -> PositionAlertState:
    import json
    from pathlib import Path

    state_path = Path(path or position_state_path())
    if not state_path.exists():
        return PositionAlertState(positions={}, leg_pnl={}, book_pnl=None)
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return PositionAlertState(positions={}, leg_pnl={}, book_pnl=None)
    previous_qty = raw.get("previous_qty") or {}
    previous_leg_pnl = raw.get("previous_leg_pnl") or {}
    book_pnl = raw.get("book_pnl")
    return PositionAlertState(
        positions={str(key): float(value) for key, value in previous_qty.items()},
        leg_pnl={str(key): float(value) for key, value in previous_leg_pnl.items()},
        book_pnl=float(book_pnl) if isinstance(book_pnl, int | float) else None,
        updated_at=str(raw.get("fetched_at")) if raw.get("fetched_at") else None,
    )


def save_position_alert_state(
    snapshot: PositionSnapshot,
    *,
    path: str | None = None,
    leg_pnl: dict[str, float] | None = None,
) -> None:
    import json
    from pathlib import Path

    state_path = Path(path or position_state_path())
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": snapshot.fetched_at,
        "previous_qty": {item.position_key: item.qty for item in snapshot.positions},
        "previous_leg_pnl": {
            item.position_key: item.unrealized_pnl
            for item in snapshot.positions
            if item.unrealized_pnl is not None
        },
        "book_pnl": snapshot.book_unrealized_pnl,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    if leg_pnl is not None:
        payload["previous_leg_pnl"] = leg_pnl
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


def book_pnl_metrics(snapshot: PositionSnapshot) -> tuple[float | None, float | None, float | None]:
    pnls = [item.unrealized_pnl for item in snapshot.positions if item.unrealized_pnl is not None]
    if not pnls:
        return None, None, None
    book_pnl = sum(pnls)
    book_cost = sum(abs(item.avg_cost) for item in snapshot.positions if item.unrealized_pnl is not None)
    book_pnl_pct = (book_pnl / book_cost * 100.0) if book_cost else None
    return book_pnl, book_cost, book_pnl_pct


def format_usd(value: float) -> str:
    return f"${value:+,.0f}"


def format_leg_line(position: SpxwPosition) -> str:
    if position.unrealized_pnl is None:
        return f"- {position.label} qty={position.qty:g} pnl=unavailable"
    pct = (
        f" ({position.unrealized_pnl_pct:+.1f}%)"
        if position.unrealized_pnl_pct is not None
        else ""
    )
    mark = f" mark={position.market_price:g}" if position.market_price is not None else ""
    return (
        f"- {position.label} qty={position.qty:g} "
        f"{format_usd(position.unrealized_pnl)}{pct}{mark}"
    )


def format_book_detail(snapshot: PositionSnapshot, *, book_pnl_pct: float | None) -> str:
    spx = snapshot.spx_reference_price
    spx_line = f"SPX {spx:g}" if spx is not None else "SPX unavailable"
    pct = f" ({book_pnl_pct:+.1f}% on cost)" if book_pnl_pct is not None else ""
    lines = [f"{spx_line}{pct}"]
    lines.extend(format_leg_line(position) for position in snapshot.positions)
    return "\n".join(lines)


def pnl_severity(book_pnl: float, *, loss_usd: float, critical_loss_usd: float) -> str:
    if book_pnl <= -critical_loss_usd:
        return "critical"
    if book_pnl <= -loss_usd:
        return "high"
    if book_pnl >= loss_usd:
        return "medium"
    return "medium"


def pnl_dedup_bucket(book_pnl: float, *, step_usd: float) -> str:
    if step_usd <= 0:
        return f"{book_pnl:.0f}"
    bucket = int(book_pnl // step_usd)
    return f"{bucket * int(step_usd)}"


def evaluate_position_alerts(
    snapshot: PositionSnapshot | None,
    *,
    previous: PositionAlertState,
    state: LatestState,
    options_map: OptionsMap | None,
    window: AlertWindow,
    persist_state: bool = False,
) -> list[Alert]:
    if snapshot is None or not snapshot.positions:
        return []
    if not env_bool("ALERT_POSITIONS_ENABLED", True):
        return []

    alerts: list[Alert] = []
    structural_enabled = env_bool("ALERT_POSITION_STRUCTURAL_ENABLED", True)
    pnl_enabled = env_bool("ALERT_POSITION_PNL_ENABLED", True)
    pnl_change_usd = env_float("ALERT_POSITION_PNL_CHANGE_USD", 200.0)
    pnl_loss_usd = env_float("ALERT_POSITION_PNL_LOSS_USD", 400.0)
    pnl_critical_loss_usd = env_float("ALERT_POSITION_PNL_CRITICAL_LOSS_USD", 1000.0)
    pnl_bucket_usd = env_float("ALERT_POSITION_PNL_DEDUP_BUCKET_USD", 100.0)

    if structural_enabled:
        alerts.extend(_structural_alerts(snapshot, previous=previous, window=window))

    if pnl_enabled:
        book_alert = _book_pnl_alert(
            snapshot,
            previous=previous,
            window=window,
            pnl_change_usd=pnl_change_usd,
            pnl_loss_usd=pnl_loss_usd,
            pnl_critical_loss_usd=pnl_critical_loss_usd,
            pnl_bucket_usd=pnl_bucket_usd,
        )
        if book_alert is not None:
            alerts.append(book_alert)

    if persist_state and snapshot is not None:
        save_position_alert_state(snapshot)
    return alerts


def _structural_alerts(
    snapshot: PositionSnapshot,
    *,
    previous: PositionAlertState,
    window: AlertWindow,
) -> list[Alert]:
    alerts: list[Alert] = []
    for position in snapshot.positions:
        prev_qty = previous.positions.get(position.position_key)
        if prev_qty is None and position.qty != 0:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_opened",
                    title=f"新开 {position.label}",
                    detail=f"SPXW 新开仓 {position.label}，数量 {position.qty:g}。",
                    severity=severity_for_priority(window.priority),
                    dedup_group="opened",
                )
            )
        elif prev_qty is not None and prev_qty != 0 and position.qty == 0:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_closed",
                    title=f"平仓 {position.label}",
                    detail=f"SPXW 已平仓 {position.label}（原数量 {prev_qty:g}）。",
                    severity=severity_for_priority(window.priority),
                    dedup_group="closed",
                )
            )
        elif prev_qty is not None and position.qty != prev_qty:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_qty_changed",
                    title=f"调仓 {position.label}",
                    detail=f"SPXW {position.label} 数量 {prev_qty:g} → {position.qty:g}。",
                    severity=severity_for_priority(window.priority),
                    dedup_group=f"{position.qty:g}",
                )
            )
    return alerts


def _book_pnl_alert(
    snapshot: PositionSnapshot,
    *,
    previous: PositionAlertState,
    window: AlertWindow,
    pnl_change_usd: float,
    pnl_loss_usd: float,
    pnl_critical_loss_usd: float,
    pnl_bucket_usd: float,
) -> Alert | None:
    book_pnl, _book_cost, book_pnl_pct = book_pnl_metrics(snapshot)
    if book_pnl is None:
        return None

    prev_book_pnl = previous.book_pnl
    bucket = pnl_dedup_bucket(book_pnl, step_usd=pnl_bucket_usd)
    prev_bucket = (
        pnl_dedup_bucket(prev_book_pnl, step_usd=pnl_bucket_usd)
        if prev_book_pnl is not None
        else None
    )
    moved_enough = (
        prev_book_pnl is not None and abs(book_pnl - prev_book_pnl) >= pnl_change_usd
    )
    bucket_changed = prev_bucket is not None and bucket != prev_bucket
    deep_loss = book_pnl <= -pnl_loss_usd

    if prev_book_pnl is None:
        if not deep_loss:
            return None
        reason = "initial_loss_threshold"
    elif moved_enough or bucket_changed or deep_loss:
        if moved_enough:
            delta = book_pnl - prev_book_pnl
            reason = f"change {format_usd(delta)} since last alert"
        elif deep_loss:
            reason = f"book loss beyond {format_usd(-pnl_loss_usd)}"
        else:
            reason = f"crossed {format_usd(float(bucket))} bucket"
    else:
        return None

    pct_text = f" ({book_pnl_pct:+.1f}%)" if book_pnl_pct is not None else ""
    title = f"SPXW 浮盈浮亏 {format_usd(book_pnl)}{pct_text}"
    detail = f"{reason}\n{format_book_detail(snapshot, book_pnl_pct=book_pnl_pct)}"
    return Alert(
        severity=pnl_severity(
            book_pnl,
            loss_usd=pnl_loss_usd,
            critical_loss_usd=pnl_critical_loss_usd,
        ),
        kind="spxw_position_book_pnl",
        instrument_id="option_map:SPXW",
        title=title,
        detail=detail,
        provider="ibkr",
        quality="live",
        value=book_pnl,
        threshold=-pnl_loss_usd if book_pnl < 0 else pnl_loss_usd,
        research_only=False,
        source_gate="ibkr_positions",
        dedup_group=bucket,
    )


def _position_alert(
    position: SpxwPosition,
    *,
    kind: str,
    title: str,
    detail: str,
    severity: str,
    dedup_group: str,
    value: float | None = None,
    threshold: float | None = None,
) -> Alert:
    return Alert(
        severity=severity,
        kind=kind,
        instrument_id=position.canonical_id,
        title=title,
        detail=detail,
        provider="ibkr",
        quality="live",
        value=value,
        threshold=threshold,
        research_only=False,
        source_gate="ibkr_positions",
        dedup_group=dedup_group,
    )
