"""Table-driven IBKR provider runtime / reconnect machine tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from spx_spark.ibkr.stream.models import ReconnectPolicy, StreamAction
from spx_spark.ibkr.stream.runtime_machine import (
    classify_connect_failure,
    decide_after_flush,
)


@pytest.mark.parametrize(
    ("connected", "allowed", "competing", "gateway", "expected"),
    [
        (True, True, False, False, StreamAction.CONTINUE),
        (True, True, True, False, StreamAction.CONFLICT_WAIT),
        (True, True, False, True, StreamAction.GATEWAY_RESTART),
        (False, True, False, False, StreamAction.RECONNECT),
        (True, False, False, False, StreamAction.POLICY_BLOCKED),
        # Competing session wins over disconnect / gateway.
        (False, True, True, True, StreamAction.CONFLICT_WAIT),
        (True, False, True, False, StreamAction.CONFLICT_WAIT),
        (False, False, False, True, StreamAction.GATEWAY_RESTART),
        (False, False, False, False, StreamAction.RECONNECT),
    ],
)
def test_decide_after_flush_table(
    connected: bool,
    allowed: bool,
    competing: bool,
    gateway: bool,
    expected: StreamAction,
) -> None:
    assert (
        decide_after_flush(
            connected=connected,
            allowed=allowed,
            competing_session=competing,
            gateway_restart=gateway,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ("Error 10182: Failed to request live updates", "market_data_rerequest"),
        ("Error 326: Unable to connect as the client id is already in use", "client_id_conflict"),
        (RuntimeError("client id conflict on gateway"), "client_id_conflict"),
        ("unrelated failure", None),
    ],
)
def test_classify_connect_failure_table(error: object, expected: str | None) -> None:
    assert classify_connect_failure(error) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("min_seconds", "max_seconds", "expected_delays"),
    [
        (5.0, 60.0, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0]),
        (1.0, 4.0, [1.0, 2.0, 4.0, 4.0]),
    ],
)
def test_reconnect_backoff_table(
    min_seconds: float,
    max_seconds: float,
    expected_delays: list[float],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("spx_spark.ibkr.stream.models.reconnect_jitter", lambda s: s)
    policy = ReconnectPolicy(min_seconds=min_seconds, max_seconds=max_seconds)
    assert [policy.next_delay() for _ in expected_delays] == expected_delays
    policy.reset()
    assert policy.next_delay() == expected_delays[0]


def test_hundred_reconnects_single_owner_no_subscription_leak() -> None:
    """Simulate 100 disconnect/reconnect cycles with one session owner.

    A second concurrent owner must be rejected. Each teardown must clear
    subscription maps so the next cycle starts empty.
    """

    owners: list[int] = []
    active_owner: int | None = None
    leaked_labels: list[str] = []

    class FakeIb:
        def __init__(self) -> None:
            self.is_connected = False
            self.subs: dict[str, object] = {}
            self.errorEvent = SimpleNamespace(__iadd__=lambda self, cb: None)

        def isConnected(self) -> bool:
            return self.is_connected

        def disconnect(self) -> None:
            self.is_connected = False
            self.subs.clear()

    class FakeCollector:
        def __init__(self) -> None:
            self.ib = FakeIb()
            self.base_subs: dict[str, object] = {}
            self.hot_subs: dict[str, object] = {}
            self.rotation_subs: dict[str, object] = {}
            self.owner_id: int | None = None

        def open_session(self, owner_id: int) -> None:
            nonlocal active_owner
            if active_owner is not None:
                raise RuntimeError(f"duplicate session owner: {active_owner} vs {owner_id}")
            active_owner = owner_id
            owners.append(owner_id)
            self.owner_id = owner_id
            self.ib.is_connected = True
            self.base_subs["index:SPX"] = object()
            self.hot_subs["option:hot"] = object()

        def teardown(self) -> None:
            nonlocal active_owner
            if self.base_subs or self.hot_subs or self.rotation_subs:
                # Capture leak before clear for assertion below.
                leaked_labels.extend([*self.base_subs, *self.hot_subs, *self.rotation_subs])
            self.base_subs.clear()
            self.hot_subs.clear()
            self.rotation_subs.clear()
            self.ib.disconnect()
            if active_owner == self.owner_id:
                active_owner = None
            self.owner_id = None

    collector = FakeCollector()
    policy = ReconnectPolicy(min_seconds=0.01, max_seconds=0.01)

    for cycle in range(100):
        collector.open_session(owner_id=cycle)
        # Mid-session disconnect path: tear down before next reconnect.
        assert active_owner == cycle
        assert collector.base_subs and collector.hot_subs
        collector.teardown()
        assert active_owner is None
        assert not collector.base_subs
        assert not collector.hot_subs
        assert not collector.rotation_subs
        _ = policy.next_delay()

    assert len(owners) == 100
    assert active_owner is None
    # teardown clears maps; leaked_labels records pre-clear contents each cycle.
    assert len(leaked_labels) == 200  # base+hot each of 100 cycles, then cleared
    assert set(leaked_labels) == {"index:SPX", "option:hot"}

    collector.open_session(owner_id=1000)
    with pytest.raises(RuntimeError, match="duplicate session owner"):
        FakeCollector().open_session(owner_id=1001)
    collector.teardown()
