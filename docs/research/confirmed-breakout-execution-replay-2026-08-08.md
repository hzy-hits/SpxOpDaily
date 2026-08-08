# Confirmed Breakout 执行回放（2026-08-08）

状态：**研究证据；不授予交易权限。**

## 问题

确认后的 level breakout 在 SPX 方向上是否存在优势，以及该优势能否覆盖 0DTE
SPXW vertical 的真实双边价成本。

## 因果口径

- 来源：Oracle `features/level_decision_outcomes` 与 normalized quote lake。
- 事件：截至当时已完成的 5 分钟 `breakout`，按 `event_id` 去重。
- 会话：只保留真实 RTH；93 个事件中 44 个为 RTH，49 个在 RTH 外。
- 合约：以确认 level 为 long strike，同方向 10 点 SPXW debit vertical。
- 入场：确认时点之前 15 秒内最新 Schwab 两腿，以组合 ask 计入。
- 退出：确认后 5 分钟之前 15 秒内最新两腿，以组合 bid 计出。
- 成本敏感性：另扣每组往返 `$4`；没有订单能力，因此不宣称真实成交。
- 全部 44 个 RTH 事件都重建出完整 entry/exit BBO，没有用 mid 补值。

## 结果

| Cohort | N | 毛胜率 | 平均毛收益 | 中位毛收益 | 扣 `$4` 后平均 |
|---|---:|---:|---:|---:|---:|
| 全部 RTH breakout | 44 | 47.7% | -$8.30 | -$5.00 | -$12.30 |
| Up | 25 | 56.0% | +$5.80 | +$10.00 | +$1.80 |
| Down | 19 | 36.8% | -$26.84 | -$20.00 | -$30.84 |
| Flip High | 11 | 63.6% | +$20.00 | +$40.00 | +$16.00 |
| Call Wall | 14 | 50.0% | -$5.36 | -$7.50 | -$9.36 |
| Flip Low | 14 | 35.7% | -$23.57 | -$15.00 | -$27.57 |
| Put Wall | 5 | 40.0% | -$36.00 | -$20.00 | -$40.00 |

`Up + Flip High` 分布在 7 个交易日。再要求确认时 SPX 到 Call Wall 至少保留
10 点，只剩 9 笔，平均毛收益约 `+$7.78`、扣成本约 `+$3.78`；8 月 6 日同一
交易日簇连续亏损，尚不能称为稳定 edge。

## 与当前生产合同的关系

这份回放使用固定 10 点候选结构，不等于历史生产曾推送的具体合约。8 月 7 日的
`7745/7750C` 是 5 点 IBKR snapshot，5 分钟保守 bid mark 为 `+$50`，但确认时
SPX 已走完约 78% 的结构目标、距离 Call Wall 不足 1 点，当前 late/target-room
门继续正确地判定 `NO_TRADE`。不得拿该事后盈利绕过入场位置。

现已允许 confirmed breakout 在 path classifier 为 `TRANSITION/UNCERTAIN` 时进入
后续候选竞争；明确反向 `TREND` 仍否决。该修改只删除重复方向授权，不把本回放
解释为可交易净收益证据。fresh exact BBO、目标空间、60% 进度、debit、ATR stop
和 P/Q utility 仍必须通过。

## 决策

1. 不允许所有 confirmed breakout 直接生成 `READY`。
2. 不硬编码 `Up + Flip High` 为生产 edge；样本和按日稳定性不足。
3. 下一份有效证据必须来自当前候选合同的 opportunity-level quote-at-risk：真实
   entry ask、5 分钟 exit bid、未成交状态和费用敏感性。
4. 在净 PnL quantile/ES selector 有当前合同标签前，HMM、Gamma 和方向命中率都只
   能作为 covariate 或 WATCH 解释，不能替代执行收益。
