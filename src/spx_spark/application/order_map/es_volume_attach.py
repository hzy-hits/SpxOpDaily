"""Side-effectful ES volume signal attachment for order-map pushes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.volume_machine import (
    es_volume_signal,
    load_es_volume_break_watch,
    load_es_volume_samples,
    save_es_volume_state,
)
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.storage import LatestState


def _primary_wall_strike(ladder: dict[str, Any] | None, side: str) -> float | None:
    if not isinstance(ladder, dict):
        return None
    walls = ladder.get("put_walls" if side == "put" else "call_walls")
    if not isinstance(walls, list) or not walls:
        return None
    first = walls[0]
    if isinstance(first, dict):
        return finite_float(first.get("strike"))
    return finite_float(first)



def attach_es_volume_signal(
    payload: dict[str, Any],
    state: LatestState,
    *,
    sample_path: str,
    now: datetime,
    persist: bool = True,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> None:
    """Compute the ES volume-price event and append the new sample.

    Side-effectful on purpose (appends to the sample file), so it runs once per
    push at the call site instead of inside the pure payload builder that the
    thin-snapshot retry loop may invoke several times.
    """
    from spx_spark.application.order_map.render import _candidate_by_play

    quote = state.best_quote("future:ES")
    cumulative = finite_float(quote.volume) if quote is not None else None
    age_ms = quote.quote_age_ms(now) if quote is not None else None
    if (
        age_ms is not None
        and age_ms > policy.es_volume_max_quote_age_seconds * 1000.0
    ):
        cumulative = None

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    spot = finite_float(underlier.get("price"))
    if spot is None:
        spot = finite_float(payload.get("es_last"))

    ladder = payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    put_wall = _primary_wall_strike(ladder, "put")
    call_wall = _primary_wall_strike(ladder, "call")
    by_play = _candidate_by_play(payload)
    if put_wall is None and "put_wall_bounce_call" in by_play:
        put_wall = finite_float(by_play["put_wall_bounce_call"].get("level"))
    if call_wall is None and "call_wall_fade_put" in by_play:
        call_wall = finite_float(by_play["call_wall_fade_put"].get("level"))
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None

    samples = load_es_volume_samples(sample_path)
    previous_watch = load_es_volume_break_watch(sample_path)
    signal = es_volume_signal(
        cumulative,
        samples,
        now=now,
        spot=spot,
        put_wall=put_wall,
        call_wall=call_wall,
        flip_zone=flip_zone,
        break_watch=previous_watch,
        policy=policy,
    )
    payload["es_volume"] = signal
    if persist and cumulative is not None:
        sample: dict[str, Any] = {"at": now.isoformat(), "volume": cumulative}
        if spot is not None:
            sample["price"] = spot
        samples.append(sample)
        new_watch = signal.get("break_watch") if isinstance(signal, dict) else previous_watch
        save_es_volume_state(
            sample_path,
            samples,
            break_watch=new_watch if isinstance(new_watch, dict) else None,
            max_samples=policy.es_volume_max_samples,
        )
