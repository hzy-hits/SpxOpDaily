from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.alert_engine import Alert, env_bool, env_float, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.ibkr.position_watcher import PositionSnapshot, SpxwPosition, position_state_path
from spx_spark.options_map import OptionsMap
from spx_spark.storage import LatestState


OPTION_GAMMA_ALERT_STATES = {
    "negative_gamma_acceleration",
    "zero_gamma_transition",
}


@dataclass(frozen=True)
class PositionAlertState:
    positions: dict[str, float]
    updated_at: str | None = None

    @classmethod
    def from_snapshot(cls, snapshot: PositionSnapshot | None) -> PositionAlertState:
        if snapshot is None:
            return cls(positions={})
        return cls(
            positions={item.position_key: item.qty for item in snapshot.positions},
            updated_at=snapshot.fetched_at,
        )


def load_position_alert_state(path: str | None = None) -> PositionAlertState:
    import json
    from pathlib import Path

    state_path = Path(path or position_state_path())
    if not state_path.exists():
        return PositionAlertState(positions={})
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return PositionAlertState(positions={})
    previous = raw.get("previous_qty") or {}
    return PositionAlertState(
        positions={str(key): float(value) for key, value in previous.items()},
        updated_at=str(raw.get("fetched_at")) if raw.get("fetched_at") else None,
    )


def save_position_alert_state(
    snapshot: PositionSnapshot,
    *,
    path: str | None = None,
) -> None:
    import json
    from pathlib import Path

    state_path = Path(path or position_state_path())
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": snapshot.fetched_at,
        "previous_qty": {item.position_key: item.qty for item in snapshot.positions},
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


def expiry_days_remaining(expiry: str, *, now: datetime) -> int | None:
    if len(expiry) != 8 or not expiry.isdigit():
        return None
    expiry_date = datetime.strptime(expiry, "%Y%m%d").replace(tzinfo=timezone.utc)
    return (expiry_date.date() - now.date()).days


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
    now = state.as_of
    pnl_threshold = env_float("ALERT_POSITION_PNL_LOSS_PCT", 25.0)
    strike_distance_threshold = env_float("ALERT_POSITION_STRIKE_DISTANCE_POINTS", 30.0)
    wall_distance_threshold = env_float("ALERT_POSITION_NEAR_WALL_POINTS", 15.0)
    expiry_days_threshold = int(env_float("ALERT_POSITION_NEAR_EXPIRY_DAYS", 1.0))

    expiry_maps = {item.expiry: item for item in options_map.expiries} if options_map else {}

    for position in snapshot.positions:
        prev_qty = previous.positions.get(position.position_key)
        if prev_qty is None and position.qty != 0:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_opened",
                    title=f"Opened {position.label}",
                    detail=f"New SPXW position {position.label} qty={position.qty:g}.",
                    severity=severity_for_priority(window.priority),
                    dedup_group="opened",
                )
            )
        elif prev_qty is not None and prev_qty != 0 and position.qty == 0:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_closed",
                    title=f"Closed {position.label}",
                    detail=f"SPXW position {position.label} is now flat (was {prev_qty:g}).",
                    severity=severity_for_priority(window.priority),
                    dedup_group="closed",
                )
            )
        elif prev_qty is not None and position.qty != prev_qty:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_qty_changed",
                    title=f"Size change {position.label}",
                    detail=f"SPXW {position.label} qty changed {prev_qty:g} -> {position.qty:g}.",
                    severity=severity_for_priority(window.priority),
                    dedup_group=f"{position.qty:g}",
                )
            )

        days_left = expiry_days_remaining(position.expiry, now=now)
        if days_left is not None and days_left <= expiry_days_threshold:
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_near_expiry",
                    title=f"{position.label} expires in {days_left}d",
                    detail=(
                        f"Held SPXW {position.label} expires on {position.expiry} "
                        f"({days_left} day(s) left); qty={position.qty:g}."
                    ),
                    severity="high" if days_left <= 0 else severity_for_priority(window.priority),
                    dedup_group=str(days_left),
                )
            )

        if position.distance_from_spx_points is not None:
            distance = position.distance_from_spx_points
            if position.right == "C":
                near_short_call = position.qty < 0 and 0 <= distance <= strike_distance_threshold
                near_long_call = position.qty > 0 and -strike_distance_threshold <= distance <= 0
                if near_short_call or near_long_call:
                    alerts.append(
                        _position_alert(
                            position,
                            kind="spxw_position_near_short_call",
                            title=f"SPX near held call {position.strike:.0f}",
                            detail=(
                                f"SPX is {distance:+.1f} pts from held {position.label}; "
                                f"qty={position.qty:g}."
                            ),
                            severity="high" if position.qty < 0 else severity_for_priority(window.priority),
                            value=distance,
                            threshold=strike_distance_threshold,
                            dedup_group=f"{position.strike:.0f}",
                        )
                    )
            if position.right == "P":
                near_short_put = position.qty < 0 and -strike_distance_threshold <= distance <= 0
                near_long_put = position.qty > 0 and 0 <= distance <= strike_distance_threshold
                if near_short_put or near_long_put:
                    alerts.append(
                        _position_alert(
                            position,
                            kind="spxw_position_near_short_put",
                            title=f"SPX near held put {position.strike:.0f}",
                            detail=(
                                f"SPX is {distance:+.1f} pts from held {position.label}; "
                                f"qty={position.qty:g}."
                            ),
                            severity="high" if position.qty < 0 else severity_for_priority(window.priority),
                            value=distance,
                            threshold=strike_distance_threshold,
                            dedup_group=f"{position.strike:.0f}",
                        )
                    )

        if (
            position.unrealized_pnl_pct is not None
            and position.unrealized_pnl_pct <= -pnl_threshold
        ):
            alerts.append(
                _position_alert(
                    position,
                    kind="spxw_position_unrealized_loss",
                    title=f"{position.label} loss {position.unrealized_pnl_pct:.1f}%",
                    detail=(
                        f"Held SPXW {position.label} unrealized PnL is "
                        f"{position.unrealized_pnl_pct:.1f}% (qty={position.qty:g})."
                    ),
                    severity="high",
                    value=position.unrealized_pnl_pct,
                    threshold=-pnl_threshold,
                    dedup_group=f"{int(position.unrealized_pnl_pct // 5) * 5}",
                )
            )

        expiry_map = expiry_maps.get(position.expiry)
        if expiry_map is not None:
            if expiry_map.gamma_state in OPTION_GAMMA_ALERT_STATES:
                alerts.append(
                    _position_alert(
                        position,
                        kind="spxw_position_gamma_regime",
                        title=f"Held SPXW in {expiry_map.gamma_state}",
                        detail=(
                            f"Held {position.label} sits in expiry {position.expiry} with "
                            f"gamma_state={expiry_map.gamma_state}."
                        ),
                        severity=severity_for_priority(window.priority),
                        dedup_group=expiry_map.gamma_state,
                    )
                )
            for wall_name, wall_strike in (
                ("call_wall", expiry_map.call_wall),
                ("put_wall", expiry_map.put_wall),
            ):
                if wall_strike is None:
                    continue
                if abs(position.strike - wall_strike) <= wall_distance_threshold:
                    alerts.append(
                        _position_alert(
                            position,
                            kind="spxw_position_near_market_wall",
                            title=f"Held strike {position.strike:.0f} near {wall_name}",
                            detail=(
                                f"Held {position.label} strike is within "
                                f"{wall_distance_threshold:.0f} pts of market {wall_name} "
                                f"{wall_strike:.0f}."
                            ),
                            severity=severity_for_priority(window.priority),
                            value=position.strike - wall_strike,
                            threshold=wall_distance_threshold,
                            dedup_group=f"{wall_name}:{wall_strike:.0f}",
                        )
                    )

    if persist_state and snapshot is not None:
        save_position_alert_state(snapshot)
    return alerts


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
