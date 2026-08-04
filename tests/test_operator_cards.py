from __future__ import annotations

import pytest

from spx_spark.notifier.operator_cards import (
    OPERATOR_SECTION_TITLES,
    render_operator_card,
)


def test_operator_card_has_one_ordered_nonempty_section_contract() -> None:
    card = render_operator_card(
        desk_view="方向与结构",
        execution="合约与触发",
        risk="止损与权限",
        targets="目标位",
        data_quality="数据状态",
    )

    offsets = [card.index(f"## {title}") for title in OPERATOR_SECTION_TITLES]
    assert offsets == sorted(offsets)
    assert card.count("## ") == len(OPERATOR_SECTION_TITLES)


def test_operator_card_rejects_an_empty_section() -> None:
    with pytest.raises(ValueError, match="Data Quality"):
        render_operator_card(
            desk_view="方向与结构",
            execution="合约与触发",
            risk="止损与权限",
            targets="目标位",
            data_quality="  ",
        )
