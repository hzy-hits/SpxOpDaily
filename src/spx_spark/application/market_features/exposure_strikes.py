"""Select a bounded, decision-relevant SPXW strike exposure map."""

from __future__ import annotations

from dataclasses import asdict

from spx_spark.features.exposure_map import ExpiryExposure, StrikeExposure

KEY_STRIKE_LIMIT = 8


def key_strike_features(
    exposure: ExpiryExposure | None,
    *,
    underlier: float | None,
    limit: int = KEY_STRIKE_LIMIT,
) -> list[dict[str, object]]:
    """Return the most relevant wall/ATM/flip strikes, sorted spatially."""

    if exposure is None or limit <= 0 or not exposure.strikes:
        return []
    if not any(_has_exposure_metrics(row) for row in exposure.strikes):
        return []

    by_strike = {row.strike: row for row in exposure.strikes}
    roles: dict[float, list[str]] = {}
    priority: dict[float, float] = {}

    def add_role(strike: float | None, label: str, weight: float) -> None:
        if strike is None or not by_strike:
            return
        selected = min(by_strike, key=lambda value: abs(value - strike))
        labels = roles.setdefault(selected, [])
        if label not in labels:
            labels.append(label)
        priority[selected] = priority.get(selected, 0.0) + weight

    for index, wall in enumerate(exposure.walls.put_walls):
        add_role(wall.strike, "主Put墙" if index == 0 else f"Put墙{index + 1}", 120 - index * 8)
    for index, wall in enumerate(exposure.walls.call_walls):
        add_role(
            wall.strike,
            "主Call墙" if index == 0 else f"Call墙{index + 1}",
            120 - index * 8,
        )

    add_role(underlier, "ATM", 110)
    add_role(exposure.zero_gamma, "ZG", 105)
    if exposure.gamma_flip_zone is not None:
        add_role(exposure.gamma_flip_zone[0], "Flip下", 95)
        add_role(exposure.gamma_flip_zone[1], "Flip上", 95)

    oi_abs_gex = exposure.oi_weighted.abs_gex or 0.0
    volume_abs_gex = exposure.volume_weighted.abs_gex or 0.0
    oi_abs_dex = exposure.oi_weighted.abs_dex_proxy or 0.0
    volume_abs_dex = exposure.volume_weighted.abs_dex_proxy or 0.0

    def magnitude_score(row: StrikeExposure) -> float:
        values = (
            _share(row.oi_weighted.abs_gex, oi_abs_gex),
            _share(row.volume_weighted.abs_gex, volume_abs_gex),
            _share(row.oi_weighted.abs_dex_proxy, oi_abs_dex),
            _share(row.volume_weighted.abs_dex_proxy, volume_abs_dex),
        )
        return 40.0 * max(values)

    ranked = sorted(
        exposure.strikes,
        key=lambda row: (
            -(priority.get(row.strike, 0.0) + magnitude_score(row)),
            abs(row.strike - underlier) if underlier is not None else 0.0,
            row.strike,
        ),
    )
    selected = sorted(ranked[:limit], key=lambda row: row.strike)
    return [
        {
            "strike": row.strike,
            "distance_points": row.strike - underlier if underlier is not None else None,
            "roles": roles.get(row.strike, []),
            "call_delta": row.call_delta,
            "put_delta": row.put_delta,
            "call_gamma": row.call_gamma,
            "put_gamma": row.put_gamma,
            "call_open_interest": row.call_open_interest,
            "put_open_interest": row.put_open_interest,
            "call_volume": row.call_volume,
            "put_volume": row.put_volume,
            "oi_weighted": asdict(row.oi_weighted),
            "volume_weighted": asdict(row.volume_weighted),
        }
        for row in selected
    ]


def _share(value: float | None, total: float) -> float:
    if value is None or total <= 0:
        return 0.0
    return abs(value) / total


def _has_exposure_metrics(row: StrikeExposure) -> bool:
    return any(
        value is not None
        for value in (
            row.call_delta,
            row.put_delta,
            row.call_gamma,
            row.put_gamma,
            row.oi_weighted.net_gex,
            row.oi_weighted.net_dex_proxy,
            row.volume_weighted.net_gex,
            row.volume_weighted.net_dex_proxy,
        )
    )
