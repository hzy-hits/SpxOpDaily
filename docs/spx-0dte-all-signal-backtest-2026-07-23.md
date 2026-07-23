# SPX 0DTE 全信号回测与 RTH 漏斗复核

研究截止：2026-07-22（exclusive cutoff `2026-07-23T00:00:00Z`）

## 结论

RTH 不是从来没有信号。7/13–7/22 的 21 个正式 `CONFIRMED` 中，RTH 有 4 个、非 RTH 有 17 个；4 个 RTH 信号的 300 秒方向结果为 3 正 1 负，baseline 裸单控制回放为 `+$885.00`。最近 7/20–7/22 才是 RTH 正式确认归零。

最近三日归零也不是行情断流。三天 RTH 都覆盖 390/390 分钟，决策健康采样约每 5 秒一次，最大间隔低于 10.1 秒。10 个 RTH lifecycle 中有 9 个被 `structure_change_pending` 立即失效，剩余 1 个因 90 秒内没有 retest 而超时。根因是结构候选与活跃事件的状态机耦合，不是 `formal_signal_enabled` 关闭，也不是 RTH 没有市场数据。

全部可重评分数据仍不足以证明可上线的新收益参数。245 组 follow-through 网格的样本内最优参数在真正 expanding walk-forward 中，后续首个有交易的会话为 `-$360.00`。因此不把样本内冠军写入生产，只修复结构生命周期语义，并保留 entry window、限价、ES 反向阻断和现有 exit。

## 数据粒度与去重

| 数据层 | 原始行 | 经济/状态粒度 | 截止日可用结论 |
|---|---:|---:|---|
| `level_decision_audit` | 1,515 | 1,508 record keys / 607 lifecycle events | 21 个首次进入 `CONFIRMED` |
| `level_decision_outcomes` | 84 | 21 events × 4 horizons | 82 complete / 2 incomplete |
| `pricing_outcomes` | 365 | 270 semantic first touches | 去除 95 个重生/重复；256 个进入最终回测 |
| `trade_intents` | 42,719 | 3 unique terminal intents | 2 个 RTH、1 个非 RTH；仅 1 个严格成交 |
| `gth_dip_reclaim` | 6 | 6 unique legacy signals | 全部非 RTH，保持 shadow |

`trade_intents` 的 42,719 行主要是每 5 秒一次的 `observing` evaluation，不能被当成 42,719 个信号。`pricing_outcomes` 使用 `first_touch_at + contract_id + play` 全局语义去重；`session_bucket=rth_close` 也不能代替 ET 时钟分类，因为其中包含 16:00 ET 之后的记录。

## 全部正式信号

| 日期 | RTH n | RTH 300s 正确 | RTH 方向均值 | 非 RTH n | 非 RTH 方向均值 |
|---|---:|---:|---:|---:|---:|
| 2026-07-13 | 0 | 0 | — | 1 | -5.26 bps |
| 2026-07-14 | 1 | 1 | +9.84 bps | 4 | -1.80 bps |
| 2026-07-15 | 3 | 2 | +0.10 bps | 4 | -0.98 bps |
| 2026-07-16 | 0 | 0 | — | 1 | +1.97 bps |
| 2026-07-17 | 0 | 0 | — | 0 | — |
| 2026-07-20 | 0 | 0 | — | 0 | — |
| 2026-07-21 | 0 | 0 | — | 2 | +0.64 bps |
| 2026-07-22 | 0 | 0 | — | 5 | -3.69 bps |
| **合计** | **4** | **3** | **+2.54 bps** | **17** | **-1.86 bps** |

4 个 RTH formal signals 的 baseline 裸单控制回放：

| 时间（ET） | 路径 | 合约 | 退出 | 一张 PnL |
|---|---|---|---|---:|
| 7/14 10:32:30 | up / flip high breakout | 7530C | profit target | +$860.00 |
| 7/15 10:54:33 | down / flip low breakout | 7560P | invalidation | -$520.00 |
| 7/15 11:50:03 | down / flip low breakout | 7560P | target wall | +$230.00 |
| 7/15 13:49:27 | up / call wall breakout | 7560C | profit target | +$315.00 |

这些是 control/proxy 回放，不是生产账户 PnL。正式 production cohort 只有 3 个 unique TradeReady，严格回放为 1 fill、2 skips，唯一成交为 `+$800.00`。

## 最近三日 RTH 漏斗

| 日期 | RTH lifecycle | testing | break pending | accepted/rejected | retest | confirmed | structure pending 失效 | quality ok |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 7/20 | 2 | 1 | 1 | 0 | 0 | 0 | 2 | 33.50% |
| 7/21 | 3 | 1 | 1 | 0 | 0 | 0 | 3 | 63.10% |
| 7/22 | 5 | 4 | 2 | 1 | 0 | 0 | 4 | 77.50% |
| **合计** | **10** | **6** | **4** | **1** | **0** | **0** | **9** | — |

这里的低 `quality ok` 主要是策略把 `structure_change_pending` 错记成行情质量失败，不代表 SPX/ES 断流。修复后，结构候选 pending 只阻止新 arm/rearm；已有 lifecycle 继续使用冻结的 stable level，候选真正 promoted 后再由现有 drift 检查决定是否失效。真实行情质量问题仍 fail closed，TTL 与 phase timeout 不暂停。

用真实 SPX/ES 路径对三个 `break_pending → structure invalidated` 事件做有限反事实重放：

- 7/20 call wall 7450 与 7/21 call wall 7500 可继续到 `CONFIRMED`；
- 7/22 flip high 7500 仍会 phase timeout；
- 前两条反事实确认后的 300 秒方向收益约为 `-2.01 bps`、`-1.40 bps`。

所以这个修复恢复的是正确的生命周期和 RTH 可观察性，不是收益提升证明；TradeReady 风控不能因此放松。

## 全量回测与参数敏感性

当前 `15s / max(2pt, 5% EM)` follow-through 对 96 个 RTH semantic touches 的结果是 5 pass、88 fail、3 unavailable。5 个 exact RTH 回放合计 `+$60.00`，均值 `+$12.00`，中位数 `+$70.00`，胜率 60.00%（Wilson 95% CI 23.10%–88.20%），只覆盖 2 个会话；去掉最佳交易后为 `-$80.00`。

245 组网格中，样本内最优是 `20s / max(0.5pt, 7.5% EM)`：

- RTH：n=14，`+$815.00`，4 个会话中 3 个为正；
- 全 session：n=15，`+$845.00`；
- 去掉最佳交易后 RTH 仍为 `+$580.00`；
- 但 expanding walk-forward 在 7/17–7/21 没有触发，7/22 两笔合计 `-$360.00`。

这是 245 选 1 后的样本内结果，不能上线。`20s/0.5pt/7.5% EM`、`20s/1pt/7.5% EM` 和 `45s/2pt` 只登记为 shadow candidates，生产继续使用当前 follow-through。

其他敏感性结论：

- 非 RTH exact confirmed：n=9、`-$575.00`、中位数 `-$170.00`、胜率 22.22%；GTH 裸单 n=3、`-$440.00`、0 胜，继续 shadow。
- entry window 从 20 秒放宽到 30 秒会使 7/22 坏 intent 成交并回放约 `-$300.00`；追价 $0.20 约为 `-$320.00`。20 秒与不追价保持不变。
- baseline confirmed 裸单 n=13、`+$310.00`，但去掉最佳交易为 `-$645.00`；trailing 为 `-$870.00`，spread10 为 `-$300.00`。不改 exit，不把 spread 推入生产。
- ES 1 分钟反向 blocker 拦截的 5 个 joined events，300 秒方向结果全部为负；保持该 blocker。
- RTH pricing proxy 中 breakout 的样本表现优于 fade，但 exact follow-through 子集会反转；`breakout-only / fade-off` 只能 shadow，不能写成生产过滤。

## 参数与产品决定

| 项目 | 决定 | 理由 |
|---|---|---|
| RTH execution boundary | 保留硬门控 | 7/22 非 RTH intent 泄漏是契约错误 |
| Structure candidate | 修复生命周期语义 | 最近 9/10 RTH lifecycle 被 pending 过早杀死 |
| Reward/risk floor | 1.00 回退到 0.25 | 1.00 会删除全部历史可量化 RTH TradeReady，且高 RR 子集方向表现更差；1.00 不是经过验证的优化 |
| Target room | 保持至少 3 点 | 与 RTH-only、RR floor 联合后历史只保留唯一实际成交；仍只是 n=1 |
| Follow-through | 生产保持 15s/2pt/5% EM | 网格冠军在 expanding walk-forward 失败 |
| Entry limit/window | 保持不追价、20 秒 | 放宽会使 7/22 坏交易成交 |
| ES reverse gate | 保持 | 历史拦截的 5 个事件全部方向错误 |
| Exit / spreads | 保持 baseline；其他 shadow | 收益集中且替代方案没有稳定改善 |
| Weekday filters | 不增加 | Monday/Tuesday/Wednesday 样本不足，避免 weekday overfit |
| GTH | 继续 shadow | exact entry/exit cohort 未达 20，历史结果为负 |

RR 0.25 不是 edge 证明，也不是鼓励低回报交易。它只撤销一个会让 RTH 系统历史上完全失去 TradeReady 的未经验证变更；RTH、target room、ES 同向、实时双边报价、限价与 20 秒窗口仍同时生效。

## 限制

7/22 前的 `level_decision_health` 没有逐帧保存 spot、ES、live levels 与 candidate payload，因此不能伪造全历史 audit-equivalent FSM 参数 sweep。本文的全量回测是对所有已持久化 signal/proxy 做 point-in-time NBBO replay；结构 pending 的三条重放仅是明确标注的 underlier counterfactual。7/23 起 schema v2 已保存未来 walk-forward 所需字段。

本地 broker statement 仍只覆盖到 7/16，缺少 7/20–7/22 的合约、数量、手续费、真实退出和人工追价记录。用户报告的约 `$12,000` 实际亏损尚不能逐笔连接到系统信号；任何回测 PnL 都不能代替账户归因。
