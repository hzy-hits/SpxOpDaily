"""Causal target selection and confirmation-space diagnostics."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.settings.market_features import MarketFeatureSettings


@dataclass(frozen=True)
class ConfirmationGeometry:
    trigger_level: float
    direction: int
    thesis: str
    confirmation_move_points: float
    expected_slippage_points: float
    minimum_remaining_space_points: float
    required_target_distance_points: float
    target_spx: float | None
    target_distance_points: float | None
    target_source: str
    target_gex: float | None = None
    target_open_interest: float | None = None

    @property
    def feasible(self) -> bool:
        return bool(
            self.target_distance_points is not None
            and self.target_distance_points >= self.required_target_distance_points
        )

    def to_dict(self) -> dict[str, object]:
        return {**asdict(self), "feasible": self.feasible}


def confirmation_geometry(
    *,
    trigger_level: float,
    direction: int,
    thesis: str,
    walls: Sequence[Mapping[str, object]],
    expected_move_points: float | None,
    feature_policy: MarketFeatureSettings,
    level_policy: LevelDecisionPolicy,
) -> ConfirmationGeometry:
    """Choose the first material outward wall that leaves confirmation room."""

    if direction not in {-1, 1}:
        raise ValueError("direction must be -1 or 1")
    if thesis not in {"breakout", "fade"}:
        raise ValueError("thesis must be breakout or fade")
    confirmation_move = (
        max(level_policy.break_buffer_points, level_policy.confirm_move_points)
        if thesis == "breakout"
        else max(level_policy.reject_points, level_policy.confirm_move_points)
    )
    required = (
        confirmation_move
        + feature_policy.trade_confirmation_slippage_points
        + feature_policy.trade_min_target_room_points
    )
    outward: list[tuple[float, Mapping[str, object]]] = []
    for row in walls:
        strike = _number(row.get("strike"))
        if strike is None:
            continue
        gex = _number(row.get("gex"))
        open_interest = _number(row.get("open_interest"))
        if not (
            (gex is not None and abs(gex) > 0) or (open_interest is not None and open_interest > 0)
        ):
            continue
        distance = direction * (strike - trigger_level)
        if distance > 0:
            outward.append((distance, row))
    outward.sort(key=lambda item: item[0])
    for distance, row in outward:
        if distance + 1e-9 < required:
            continue
        strike = float(row["strike"])
        return ConfirmationGeometry(
            trigger_level=trigger_level,
            direction=direction,
            thesis=thesis,
            confirmation_move_points=confirmation_move,
            expected_slippage_points=feature_policy.trade_confirmation_slippage_points,
            minimum_remaining_space_points=feature_policy.trade_min_target_room_points,
            required_target_distance_points=required,
            target_spx=strike,
            target_distance_points=distance,
            target_source="gex_oi_wall_ladder",
            target_gex=_number(row.get("gex")),
            target_open_interest=_number(row.get("open_interest")),
        )

    em_distance = (
        float(expected_move_points) * feature_policy.trade_target_em_fraction
        if expected_move_points is not None
        and math.isfinite(expected_move_points)
        and expected_move_points > 0
        else None
    )
    if em_distance is not None:
        fallback_distance = max(em_distance, required)
        return ConfirmationGeometry(
            trigger_level=trigger_level,
            direction=direction,
            thesis=thesis,
            confirmation_move_points=confirmation_move,
            expected_slippage_points=feature_policy.trade_confirmation_slippage_points,
            minimum_remaining_space_points=feature_policy.trade_min_target_room_points,
            required_target_distance_points=required,
            target_spx=trigger_level + direction * fallback_distance,
            target_distance_points=fallback_distance,
            target_source="expected_move_confirmation_floor_fallback",
        )
    return ConfirmationGeometry(
        trigger_level=trigger_level,
        direction=direction,
        thesis=thesis,
        confirmation_move_points=confirmation_move,
        expected_slippage_points=feature_policy.trade_confirmation_slippage_points,
        minimum_remaining_space_points=feature_policy.trade_min_target_room_points,
        required_target_distance_points=required,
        target_spx=None,
        target_distance_points=None,
        target_source="no_valid_structural_target",
    )


def _number(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None
