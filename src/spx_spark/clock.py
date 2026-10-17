"""Clock port for decision and analytics boundaries.

Wall-clock reads stay in this module (or in the process entry points that inject
a clock). Domain and analytics code accepts ``now``/a ``Clock`` instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from time import monotonic
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def monotonic(self) -> float:
        return monotonic()


class FixedClock:
    def __init__(self, now: datetime) -> None:
        if now.tzinfo is None:
            raise ValueError("FixedClock requires an aware datetime")
        self._now = now.astimezone(timezone.utc)
        self._monotonic = 0.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        from datetime import timedelta

        self._now += timedelta(seconds=seconds)
        self._monotonic += seconds
