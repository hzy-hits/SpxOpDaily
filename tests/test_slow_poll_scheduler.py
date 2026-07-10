from __future__ import annotations

from spx_spark.ibkr.slow_poll import SlowPollAction, SlowPollScheduler


def test_four_chunks_are_spread_across_three_hundred_second_cycle() -> None:
    scheduler = SlowPollScheduler(chunk_count=4, cycle_seconds=300.0, hold_seconds=10.0)
    scheduler.reset(now=0.0)

    assert scheduler.advance(now=0.0).action is SlowPollAction.START
    assert scheduler.advance(now=5.0).action is SlowPollAction.NONE
    assert scheduler.advance(now=10.0).action is SlowPollAction.FINISH
    assert scheduler.advance(now=74.9).action is SlowPollAction.NONE

    starts = []
    for now in (75.0, 150.0, 225.0, 300.0):
        step = scheduler.advance(now=now)
        starts.append((now, step.chunk_index))
        assert step.action is SlowPollAction.START
        assert scheduler.advance(now=now + 10.0).action is SlowPollAction.FINISH

    assert starts == [(75.0, 1), (150.0, 2), (225.0, 3), (300.0, 0)]


def test_scheduler_never_blocks_between_start_and_finish() -> None:
    scheduler = SlowPollScheduler(chunk_count=1, cycle_seconds=60.0, hold_seconds=10.0)
    scheduler.reset(now=100.0)

    started = scheduler.advance(now=100.0)
    holding = scheduler.advance(now=105.0)
    finished = scheduler.advance(now=110.0)

    assert started.action is SlowPollAction.START
    assert holding.action is SlowPollAction.NONE
    assert finished.action is SlowPollAction.FINISH


def test_abort_retries_later_without_advancing_hot_loop() -> None:
    scheduler = SlowPollScheduler(chunk_count=2, cycle_seconds=100.0, hold_seconds=10.0)
    scheduler.reset(now=0.0)
    assert scheduler.advance(now=0.0).action is SlowPollAction.START

    scheduler.abort_active(now=1.0, retry_after_seconds=30.0)

    assert scheduler.advance(now=30.9).action is SlowPollAction.NONE
    retry = scheduler.advance(now=31.0)
    assert retry.action is SlowPollAction.START
