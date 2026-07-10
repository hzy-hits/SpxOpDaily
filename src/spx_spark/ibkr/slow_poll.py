from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SlowPollAction(str, Enum):
    NONE = "none"
    START = "start"
    FINISH = "finish"


@dataclass(frozen=True)
class SlowPollStep:
    action: SlowPollAction
    chunk_index: int | None = None


@dataclass
class SlowPollScheduler:
    chunk_count: int
    cycle_seconds: float = 300.0
    hold_seconds: float = 10.0
    next_chunk_index: int = 0
    active_chunk_index: int | None = None
    hold_deadline: float | None = None
    next_start_at: float | None = None

    @property
    def spacing_seconds(self) -> float:
        if self.chunk_count <= 0:
            return self.cycle_seconds
        return self.cycle_seconds / self.chunk_count

    def reset(self, *, now: float) -> None:
        self.next_chunk_index = 0
        self.active_chunk_index = None
        self.hold_deadline = None
        self.next_start_at = now

    def advance(self, *, now: float) -> SlowPollStep:
        if self.chunk_count <= 0:
            return SlowPollStep(SlowPollAction.NONE)
        if self.active_chunk_index is not None:
            if self.hold_deadline is None or now < self.hold_deadline:
                return SlowPollStep(SlowPollAction.NONE)
            completed = self.active_chunk_index
            self.active_chunk_index = None
            self.hold_deadline = None
            return SlowPollStep(SlowPollAction.FINISH, completed)

        if self.next_start_at is None:
            self.next_start_at = now
        if now < self.next_start_at:
            return SlowPollStep(SlowPollAction.NONE)

        chunk_index = self.next_chunk_index
        self.next_chunk_index = (self.next_chunk_index + 1) % self.chunk_count
        self.active_chunk_index = chunk_index
        self.hold_deadline = now + max(self.hold_seconds, 0.0)
        self.next_start_at = now + max(self.spacing_seconds, 0.0)
        return SlowPollStep(SlowPollAction.START, chunk_index)

    def abort_active(
        self,
        *,
        now: float,
        retry_after_seconds: float,
        retry_same_chunk: bool = True,
    ) -> None:
        if retry_same_chunk and self.active_chunk_index is not None:
            self.next_chunk_index = self.active_chunk_index
        self.active_chunk_index = None
        self.hold_deadline = None
        self.next_start_at = now + max(retry_after_seconds, 0.0)
