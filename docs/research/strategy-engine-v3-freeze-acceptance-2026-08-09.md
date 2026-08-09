# Strategy Engine v3 · Pass-B 回填与冻结验收 · 2026-08-05..08

状态：**工程验收报告**；`ev_hard_gate=false`（合同 §7.4 未满足，不得升门）。

## 范围

- policy: `strategy_policy.bootstrap.v2`
- management_policy: `management_policy.v1`
- 数据：`/srv/data/spx-spark/data` + `/srv/data/spx-spark/spx.sqlite`
- Pass-B：用决策时点 `market_facts` + Schwab quote lake 重建候选，再用 ManagementPolicy 打标
- 已知偏差：Pass-B 候选是重建假想集，不等于当时生产真实候选

## 冻结案例

| 日期 | PASS | 关键检查 |
|---|---|---|
| 2026-08-05 | PASS | terminal=PIN_MIGRATING（期望 PIN_MIGRATING） |
| 2026-08-06 | PASS | {'decision_type': 'CALL_BUTTERFLY', 'center': 7710.0, 'width': 10.0, 'action_authority': 'manual', 'automatic_ordering': False} |
| 2026-08-07 | PASS | Pass-B sampled=6 rebuilt=6 candidates=54 vertical_exact_reappear=0 labeled=54 |
| 2026-08-08 | PASS | control no_trade; reasons={'market_frame_not_ready': 94, 'pricing_not_authorized': 15383} |

## 验收门摘要

```json
{
  "aug5_pass": true,
  "aug6_pass": true,
  "aug7_labeled": 54,
  "aug7_pass_b_candidates": 54,
  "aug7_vertical_exact_reappear": 0,
  "aug8_control_no_trade": true,
  "ev_promotion_blocked": true
}
```

## 8/7 生产原因基线（重建前）

```json
{
  "candidate_probability_event_mismatch": 209,
  "confirmed_price_trigger_unavailable": 947,
  "price_trigger_not_aligned_with_supported_setup": 22,
  "pricing_not_authorized": 466,
  "session_not_open_for_spxw_strategy": 505,
  "vertical_exact_spread_unavailable": 27
}
```

## EV 升门

本次**不**将 ManagementPolicy EV 升为硬门。校准脚本与标签仅用于排序研究；
升门仍需 §7.4 证据与另一次明确批准。

