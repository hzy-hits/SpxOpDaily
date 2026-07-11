from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.config import IbkrPositionSettings, NotificationSettings, env_bool, env_float
from spx_spark.ibkr.position_watcher import (
    PositionSnapshot,
    SpxwPosition,
    load_snapshot,
    position_state_path,
    snapshot_book_metrics,
)
from spx_spark.options_map import OptionsMap
from spx_spark.notifier.state import load_acknowledged_event_ids
from spx_spark.position_events import (
    BOOK_PNL_EVENT_KIND,
    ObservedPosition,
    PendingPositionEvent,
    PositionEventStore,
    PositionEventStoreCorrupt,
    PositionObservation,
)
from spx_spark.runtime_config import runtime_value
from spx_spark.state_io import atomic_write_json_secure
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
    if not env_bool("ALERT_POSITIONS_ENABLED", bool(runtime_value("position_alerts.enabled"))):
        return []
    snapshot = load_snapshot(position_settings.snapshot_path)
    notification_settings = NotificationSettings.from_env()
    acknowledged_event_ids = load_acknowledged_event_ids(notification_settings.state_path)
    store = PositionEventStore(position_settings.state_path)
    try:
        batch = store.prepare(
            snapshot_to_observation(snapshot),
            acknowledged_event_ids=acknowledged_event_ids,
            as_of=state.as_of,
            max_snapshot_age_seconds=position_settings.max_snapshot_age_seconds,
            pnl_change_usd=env_float(
                "ALERT_POSITION_PNL_CHANGE_USD",
                float(runtime_value("position_alerts.pnl_change_usd")),
            ),
            pnl_loss_usd=env_float(
                "ALERT_POSITION_PNL_LOSS_USD",
                float(runtime_value("position_alerts.pnl_loss_usd")),
            ),
            pnl_critical_loss_usd=env_float(
                "ALERT_POSITION_PNL_CRITICAL_LOSS_USD",
                float(runtime_value("position_alerts.pnl_critical_loss_usd")),
            ),
            pnl_bucket_usd=env_float(
                "ALERT_POSITION_PNL_DEDUP_BUCKET_USD",
                float(runtime_value("position_alerts.pnl_bucket_usd")),
            ),
            structural_enabled=env_bool(
                "ALERT_POSITION_STRUCTURAL_ENABLED",
                bool(runtime_value("position_alerts.structural_enabled")),
            ),
            pnl_enabled=env_bool(
                "ALERT_POSITION_PNL_ENABLED",
                bool(runtime_value("position_alerts.pnl_enabled")),
            ),
        )
    except PositionEventStoreCorrupt as exc:
        return [
            Alert(
                severity="critical",
                kind="spxw_position_event_store_corrupt",
                instrument_id="option_map:SPXW",
                title="SPXW 持仓事件状态损坏",
                detail=str(exc),
                provider="internal",
                quality="error",
                research_only=False,
                source_gate="ibkr_positions",
                dedup_group="position_event_store_corrupt",
            )
        ]
    return [render_position_event(event, window=window) for event in batch.pending_events]


def snapshot_to_observation(snapshot: PositionSnapshot | None) -> PositionObservation | None:
    if snapshot is None:
        return None
    book_detail = format_book_detail(
        snapshot,
        book_pnl_pct=snapshot.book_unrealized_pnl_pct,
    )
    return PositionObservation(
        snapshot_id=snapshot.snapshot_id,
        observed_at=snapshot.fetched_at,
        fetch_complete=snapshot.fetch_complete,
        positions=tuple(
            ObservedPosition(
                key=position.position_key,
                instrument_id=position.canonical_id,
                label=position.label,
                qty=position.qty,
            )
            for position in snapshot.positions
            if position.qty != 0
        ),
        book_pnl=snapshot.book_unrealized_pnl,
        book_pnl_pct=snapshot.book_unrealized_pnl_pct,
        book_pnl_complete=snapshot.book_pnl_complete,
        book_detail=book_detail,
    )


def render_position_event(
    event: PendingPositionEvent,
    *,
    window: AlertWindow,
) -> Alert:
    if event.kind == BOOK_PNL_EVENT_KIND:
        book_pnl = event.book_pnl or 0.0
        pct_text = (
            f" ({event.book_pnl_pct:+.1f}%)" if event.book_pnl_pct is not None else ""
        )
        return Alert(
            severity=event.severity or "high",
            kind=event.kind,
            instrument_id=event.instrument_id,
            title=f"SPXW 浮盈浮亏 {format_usd(book_pnl)}{pct_text}",
            detail=event.book_detail or "SPXW book detail unavailable",
            provider="ibkr",
            quality="live",
            value=book_pnl,
            threshold=event.threshold,
            research_only=False,
            source_gate="ibkr_positions",
            dedup_group=event.pnl_bucket,
            event_id=event.event_id,
        )

    severity = severity_for_priority(window.priority)
    old_qty = event.old_qty or 0.0
    new_qty = event.new_qty or 0.0
    if event.kind == "spxw_position_opened":
        title = f"新开 {event.label}"
        detail = f"SPXW 新开仓 {event.label}，数量 {new_qty:g}。"
    elif event.kind == "spxw_position_closed":
        title = f"平仓 {event.label}"
        detail = f"SPXW 已平仓 {event.label}（原数量 {old_qty:g}）。"
    else:
        title = f"调仓 {event.label}"
        detail = f"SPXW {event.label} 数量 {old_qty:g} → {new_qty:g}。"
    return Alert(
        severity=severity,
        kind=event.kind,
        instrument_id=event.instrument_id,
        title=title,
        detail=detail,
        provider="ibkr",
        quality="live",
        value=new_qty,
        research_only=False,
        source_gate="ibkr_positions",
        dedup_group=event.event_id,
        event_id=event.event_id,
    )


def reconcile_position_event_acknowledgements(event_ids: tuple[str, ...]) -> bool:
    if not event_ids:
        return True
    position_settings = IbkrPositionSettings.from_env()
    try:
        PositionEventStore(position_settings.state_path).acknowledge(event_ids)
    except PositionEventStoreCorrupt:
        return False
    return True


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
    from pathlib import Path

    state_path = Path(path or position_state_path())
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
    atomic_write_json_secure(state_path, payload)


def book_pnl_metrics(snapshot: PositionSnapshot) -> tuple[float | None, float | None, float | None]:
    if not snapshot.book_pnl_complete:
        return None, None, None
    return snapshot_book_metrics(snapshot.positions)


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
    # None = fetch failed / snapshot missing — do not invent closes or wipe state.
    if snapshot is None:
        return []
    if not snapshot.fetch_complete:
        return []
    if not env_bool("ALERT_POSITIONS_ENABLED", bool(runtime_value("position_alerts.enabled"))):
        return []

    alerts: list[Alert] = []
    structural_enabled = env_bool(
        "ALERT_POSITION_STRUCTURAL_ENABLED",
        bool(runtime_value("position_alerts.structural_enabled")),
    )
    pnl_enabled = env_bool(
        "ALERT_POSITION_PNL_ENABLED", bool(runtime_value("position_alerts.pnl_enabled"))
    )
    pnl_change_usd = env_float(
        "ALERT_POSITION_PNL_CHANGE_USD",
        float(runtime_value("position_alerts.pnl_change_usd")),
    )
    pnl_loss_usd = env_float(
        "ALERT_POSITION_PNL_LOSS_USD",
        float(runtime_value("position_alerts.pnl_loss_usd")),
    )
    pnl_critical_loss_usd = env_float(
        "ALERT_POSITION_PNL_CRITICAL_LOSS_USD",
        float(runtime_value("position_alerts.pnl_critical_loss_usd")),
    )
    pnl_bucket_usd = env_float(
        "ALERT_POSITION_PNL_DEDUP_BUCKET_USD",
        float(runtime_value("position_alerts.pnl_bucket_usd")),
    )

    if structural_enabled:
        alerts.extend(_structural_alerts(snapshot, previous=previous, window=window))

    # PnL alerts need live legs; empty book after a full flat is structural-only.
    if pnl_enabled and snapshot.positions:
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

    if persist_state:
        save_position_alert_state(snapshot)
    return alerts


def _parse_position_key(position_key: str) -> tuple[str, str] | None:
    account, sep, canonical_id = position_key.partition("|")
    if not sep or not account or not canonical_id:
        return None
    return account, canonical_id


def _stub_position_from_key(position_key: str, qty: float) -> SpxwPosition | None:
    """Rebuild a minimal SpxwPosition for legs that disappeared from the snapshot.

    The watcher drops qty==0 rows, so full/partial flats only show up as missing
    keys versus previous state — we still need a label/instrument_id for alerts.
    """
    parsed = _parse_position_key(position_key)
    if parsed is None:
        return None
    account, canonical_id = parsed
    parts = canonical_id.split(":")
    # option:SPX:SPXW:YYYYMMDD:strike:RIGHT
    if len(parts) < 6 or parts[0] != "option":
        return None
    try:
        strike = float(parts[4])
    except ValueError:
        return None
    right = parts[5].upper()
    expiry = parts[3]
    return SpxwPosition(
        account=account,
        symbol=parts[1],
        expiry=expiry,
        strike=strike,
        right=right,
        qty=qty,
        avg_cost=0.0,
        con_id=0,
        trading_class=parts[2],
        local_symbol=None,
        canonical_id=canonical_id,
    )


def _structural_alerts(
    snapshot: PositionSnapshot,
    *,
    previous: PositionAlertState,
    window: AlertWindow,
) -> list[Alert]:
    alerts: list[Alert] = []
    current_keys = {position.position_key for position in snapshot.positions}
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

    # Legs that vanished from the IB snapshot (qty filtered to 0) are closes.
    for position_key, prev_qty in previous.positions.items():
        if position_key in current_keys or prev_qty == 0:
            continue
        stub = _stub_position_from_key(position_key, 0.0)
        if stub is None:
            continue
        alerts.append(
            _position_alert(
                stub,
                kind="spxw_position_closed",
                title=f"平仓 {stub.label}",
                detail=f"SPXW 已平仓 {stub.label}（原数量 {prev_qty:g}）。",
                severity=severity_for_priority(window.priority),
                dedup_group="closed",
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
