from datetime import datetime, timezone

from spx_spark.macro_event_clock import macro_event_state


def test_cpi_pre_and_post_event_modes() -> None:
    pre = macro_event_state(datetime(2026, 7, 14, 12, 15, tzinfo=timezone.utc))
    post = macro_event_state(datetime(2026, 7, 14, 12, 45, tzinfo=timezone.utc))
    normal = macro_event_state(datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc))
    assert pre["mode"] == "pre_event"
    assert pre["entry_allowed"] is False
    assert post["mode"] == "post_event"
    assert post["entry_allowed"] is True
    assert normal["mode"] == "normal"
