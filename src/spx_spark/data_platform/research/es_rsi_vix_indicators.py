"""RSI variants and non-overlapping signal extraction for ES research."""

from __future__ import annotations

import numpy as np


def _rsi_value(average_gain: float, average_loss: float) -> float:
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    return 100.0 - 100.0 / (1.0 + average_gain / average_loss)


def wilder_rsi(prices: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(prices), np.nan)
    start = 0
    while start < len(prices):
        while start < len(prices) and not np.isfinite(prices[start]):
            start += 1
        end = start
        while end < len(prices) and np.isfinite(prices[end]):
            end += 1
        if end - start > period:
            deltas = np.diff(prices[start:end])
            gains = np.maximum(deltas, 0.0)
            losses = np.maximum(-deltas, 0.0)
            average_gain = float(gains[:period].mean())
            average_loss = float(losses[:period].mean())
            result[start + period] = _rsi_value(average_gain, average_loss)
            for offset in range(period + 1, end - start):
                average_gain = (average_gain * (period - 1) + gains[offset - 1]) / period
                average_loss = (average_loss * (period - 1) + losses[offset - 1]) / period
                result[start + offset] = _rsi_value(average_gain, average_loss)
        start = max(end, start + 1)
    return result


def cutler_rsi(prices: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(prices), np.nan)
    for index in range(period, len(prices)):
        window = prices[index - period : index + 1]
        if not np.isfinite(window).all():
            continue
        deltas = np.diff(window)
        result[index] = _rsi_value(
            float(np.maximum(deltas, 0.0).mean()),
            float(np.maximum(-deltas, 0.0).mean()),
        )
    return result


def volatility_normalized_rsi(
    prices: np.ndarray,
    *,
    period: int = 14,
    volatility_window: int = 20,
) -> np.ndarray:
    normalized_path = np.full(len(prices), np.nan)
    normalized_path[0] = 0.0 if np.isfinite(prices[0]) else np.nan
    level = 0.0
    for index in range(1, len(prices)):
        start = index - volatility_window
        if start < 0 or not np.isfinite(prices[index - 1 : index + 1]).all():
            continue
        window = prices[start : index + 1]
        if not np.isfinite(window).all():
            continue
        deltas = np.diff(window)
        scale = float(deltas.std())
        if scale <= 1e-12:
            continue
        level += float((prices[index] - prices[index - 1]) / scale)
        normalized_path[index] = level
    return wilder_rsi(normalized_path, period)


def _threshold_direction(values: np.ndarray, upper: float, lower: float) -> np.ndarray:
    direction = np.zeros(len(values), dtype=np.int8)
    direction[np.isfinite(values) & (values >= upper)] = 1
    direction[np.isfinite(values) & (values <= lower)] = -1
    return direction


def indicator_directions(prices: np.ndarray) -> dict[str, np.ndarray]:
    rsi7 = wilder_rsi(prices, 7)
    rsi14 = wilder_rsi(prices, 14)
    rsi21 = wilder_rsi(prices, 21)
    cutler14 = cutler_rsi(prices, 14)
    normalized14 = volatility_normalized_rsi(prices)

    momentum = np.zeros(len(prices), dtype=np.int8)
    for index in range(5, len(prices)):
        if not np.isfinite(prices[[index - 5, index]]).all():
            continue
        change = prices[index] - prices[index - 5]
        if change >= 1.0:
            momentum[index] = 1
        elif change <= -1.0:
            momentum[index] = -1

    velocity = np.zeros(len(prices), dtype=np.int8)
    for index in range(3, len(prices)):
        if not np.isfinite(rsi14[[index - 3, index]]).all():
            continue
        slope = rsi14[index] - rsi14[index - 3]
        if rsi14[index] >= 52 and slope >= 3:
            velocity[index] = 1
        elif rsi14[index] <= 48 and slope <= -3:
            velocity[index] = -1

    range_shift = np.zeros(len(prices), dtype=np.int8)
    for index in range(9, len(prices)):
        window = rsi14[index - 9 : index + 1]
        if not np.isfinite(window).all():
            continue
        if rsi14[index] >= 55 and float(window.min()) >= 40:
            range_shift[index] = 1
        elif rsi14[index] <= 45 and float(window.max()) <= 60:
            range_shift[index] = -1

    return {
        "momentum_5m": momentum,
        "wilder_rsi_7": _threshold_direction(rsi7, 60, 40),
        "wilder_rsi_14": _threshold_direction(rsi14, 55, 45),
        "wilder_rsi_21": _threshold_direction(rsi21, 55, 45),
        "cutler_rsi_14": _threshold_direction(cutler14, 55, 45),
        "rsi_velocity_14": velocity,
        "rsi_range_shift_14": range_shift,
        "rsi_vol_normalized_14": _threshold_direction(normalized14, 55, 45),
    }


def signal_events(
    direction: np.ndarray,
    *,
    persistence: int = 2,
    neutral_reset: int = 2,
    cooldown_minutes: int = 10,
) -> list[tuple[int, int]]:
    """Emit one event per armed direction instead of one event per minute."""

    events: list[tuple[int, int]] = []
    active = 0
    same_count = 0
    neutral_count = 0
    previous = 0
    last_event = -cooldown_minutes
    for index, raw in enumerate(direction):
        value = int(raw)
        if value == 0:
            neutral_count += 1
            same_count = 0
            previous = 0
            if neutral_count >= neutral_reset:
                active = 0
            continue
        neutral_count = 0
        same_count = same_count + 1 if value == previous else 1
        previous = value
        if (
            same_count >= persistence
            and value != active
            and index - last_event >= cooldown_minutes
        ):
            events.append((index, value))
            active = value
            last_event = index
    return events
