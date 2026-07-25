# RTH 5 分钟市场状态与 Spring Gamma v3 Shadow

当前规则版本：`market_state_5m_eight_variable_rules.v2`（2026-07-25）。

## 目标

这套规则把“市场环境”和“能否执行期权”拆成两层：

1. 用 ES 5 分钟路径和宽度识别当前 RTH 环境。
2. 等价格到达真实、同到期日的 SPXW 稳定墙位。
3. 等 level path 给出互斥触发确认。
4. 只有实时双腿 NBBO、IV、Greeks、期限和剩余空间都通过时，才展示期权表达。

市场状态本身不是可交易 Alpha，也不会直接生成订单。当前实现保持
`mode=shadow`、`action_authority=none`、`actionable=false`。

## 数据流

```text
Schwab/IBKR live ES
  -> 5 秒无重复 source timestamp 采样
  -> 不补洞的 5 分钟 OHLC
  -> 8 个 RTH 输入
  -> D / Q / V 与六类状态
  -> 稳定 0DTE 墙位位置
  -> level path 触发确认
  -> 实时 SPXW option overlay
  -> Call/Put debit spread Shadow 或明确不可用
```

状态与 option overlay 独立保存。期权链暂时不可用时，报告仍应显示可验证的
ES 市场状态，但不得生成期限、概率、价差腿或限价。八变量状态只有在
`status=ready` 且 `8/8 complete` 时才参与 Spring 确认 gate。若七个方向输入
完整而仅缺 `same_time_range_ratio`，趋势规则可以输出
`status=provisional`、`classification_tier=directional_provisional`；
它只读展示且不参与确认或否决。其他 warming/`UNCERTAIN` 同样只作诊断。

## 八个输入

| 输入 | 生产定义 | 缺失处理 |
|---|---|---|
| `price_vs_vwap` | 最近两根完整 5m close 相对 RTH VWAP；明显阈值为 `0.30 × ATR5m` | 不以“中性”代替 |
| `vwap_slope` | `(VWAP_t - VWAP_t-3) / ATR5m` | 不回退到 Globex VWAP |
| `opening_range_state` | 09:30–09:45 ET 三根完整 K 线；突破只认 close，连续两根才确认 | 缺任一 opening bar 即不可用 |
| `market_structure` | 最近六根完整 5m bar 的前后三段高低点，判断 HH/HL、LH/LL 或重叠 | 不从 1m 点样本伪造 |
| `efficiency_ratio` | 六个 5m 区间，以首根 open 加六根 close 计算净移动/总路径 | 路径不足 30 分钟即不可用 |
| `vwap_cross_count` | 最近六根 5m close 的 RTH VWAP 侧别切换次数 | 不插值缺失 close/VWAP |
| `same_time_range_ratio` | 当日 09:30 至当前的 range / 过去最多 20 个 session 同时刻 range 中位数 | 少于 10 个历史 session 保持 warming |
| `breadth_above_vwap` | 11 个 S&P sector ETF 中高于各自 RTH VWAP 的比例，至少 8 个可用 | 明确标为 sector proxy，不冒充 500 成分股 breadth |

ATR 使用最多 14 个无 gap 的完整 5m true range，最少需要 6 个。NBBO 不参与
ES bar，也绝不对不存在的 option bid/ask 做插值。

一根 bar 只有在样本数、最大样本间隔、首端覆盖和尾端覆盖全部通过时才是 `ok`。
30 分钟窗口必须是连续 5 分钟网格；缺一档就保持不可用，不会删除缺档后把两段
行情压缩成一个“连续”窗口。同刻当日 range 也必须从 09:30 连续覆盖到当前。

累计成交量 VWAP 遇到超过 135 秒的源采样洞时，不把跨洞成交量绑定到恢复点价格，
也不插值。洞内 freshness 失败；后续只累计可归属的 volume delta，并在观测成交量
占已知成交量至少 80% 后恢复发布。provider 选择先要求当前新鲜，再比较样本密度，
避免“历史点更多但已经 stale”的源压过新鲜备用源。

评分器从 09:45 ET 开始允许评估；由于 30 分钟 ER 和结构需要完整路径，正常
情况下八项最早约在 10:00 ET 全部 ready。此前输出 `UNCERTAIN`，不会强迫给方向。

## D、Q、V

方向分数 D 只由五个 `-2..+2` 分项相加：

- 价格相对 RTH VWAP；
- RTH VWAP 斜率；
- opening range 接受状态；
- 30 分钟价格结构；
- sector breadth proxy。

D 的范围固定为 `-10..+10`。ER、VWAP 穿越和同刻 range 不重复加入 D：

- Q 保留 ER 与穿越次数，区分干净路径和来回扫。
- V 保留同刻 range ratio，区分低、正常、高和极端波动。

首版分类为：

| 状态 | 规则 |
|---|---|
| `TREND_UP` | `D >= 6`、`ER > 0.45`、VWAP 穿越不超过 2；仅缺 range 时可只读 provisional |
| `TREND_DOWN` | `D <= -6`、`ER > 0.45`、VWAP 穿越不超过 2；仅缺 range 时可只读 provisional |
| `HIGH_VOL_CHOP` | `ER < 0.25` 且同刻 range ratio `> 1.25` |
| `LOW_VOL_RANGE` | `abs(D) <= 2`、`ER < 0.25`、同刻 range ratio `< 0.75` |
| `LOW_VOL_PIN` | 当前禁止直接发出 |
| `UNCERTAIN` | 方向输入不完整、波动状态缺 range、时间门未到或没有规则匹配 |

`LOW_VOL_PIN` 还需要整数 strike 邻近度与实时 ATM 跨式持续衰减确认。当前只有
`pin_proxy_candidate`，主状态仍保守显示 `LOW_VOL_RANGE`。

## 状态到表达的约束

报告固定按以下顺序显示：

```text
状态 -> 等待位置 -> 触发确认 -> 期权结构
```

- `TREND_UP`：只标出 VWAP/ORH 与上涨腿回撤观察区；本层不计算回撤比例，也不
  选择 Call 价差。
- `TREND_DOWN`：只标出 VWAP/ORL 与下跌腿反弹观察区；本层不计算反弹比例，也不
  选择 Put 价差。
- `LOW_VOL_RANGE`：只标出实时墙位边缘这一观察位置。
- `HIGH_VOL_CHOP`：只显示高波动、低效率环境标签。
- `UNCERTAIN`：状态层不生成方向、期限或期权表达。

上述“等待位置”是只读 playbook 标签，不等于 pullback 已被代码确认。正式触发
仍由冻结稳定结构上的 level lifecycle、方向一致性和剩余空间 gate 决定。报告中
的 Call/Put 只允许作为方向映射；具体 spread 必须由独立的实时双腿 Shadow 给出。

## 0DTE、1DTE 与概率

墙位路径始终来自实时、精确到期日的 front 0DTE 结构；不能用 1DTE 墙替代。
期权表达层沿用现有 tenor shadow：

- 当前人工策略只表达当日 0DTE，并在 13:00 ET 强制退出；1DTE 仅保留为期限结构、IV 与 theta 基准；
- 13:00 ET 后不再生成该策略的新机会；0DTE/1DTE 数据可继续作为只读 shadow；
- 当日 0DTE 不可用或持有窗口跨过到期结算时，按 fail-closed policy 显示不可用；
  1DTE 不得作为交易表达回退。

墙位概率使用实时 IV 的风险中性启发式，不能把 D 分数解释成胜率。真实期权表达
还必须通过精确到期日、quote age、双腿严格 NBBO、IV/Delta/Greek 覆盖和
reward/risk gate。

## 生产状态与审计文件

- `latest/es_bars_5m.json`：provider-neutral ES 5m 采样状态。
- `latest/market_state_5m_range_baselines.json`：同刻累计 range 基线。
- `latest/minute_market_frame.json`：
  `diagnostics.rth_market_state` 保存八项、D/Q/V、缺失原因和 lineage。
- `latest/spring_gamma_v3_shadow.json`：保存独立 `rth_market_state` 和
  `option_overlay`。
- 15 分钟报告：展示状态、等待位置、触发和期权结构，数值最多两位小数。

历史重放必须逐日隔离状态，并只使用当时及以前可见的数据。旧 lifecycle bug
期间的派生 phase 不能直接拿来调参；应从原始 quote lake 用当前逻辑重建。
验收固定保留两本账：`as_collected` 用 `received_at/source_at` 验证当时生产真正
知道什么；`strategy_research` 用 bar-end 因果时钟和明确标记的历史回填研究规则。
历史回填不能反向冒充当时 live pipeline 已经正常交付。

## 已知边界

- 当前生产同刻 range 已达到全日最低 10-session 门槛；少于 20 个 session 时仍标为
  `partial`，不会伪装为 mature。
- 7 月 20～22 日旧 signal lifecycle 污染了已有派生标签；原始行情仍可重放。
- 7 月 20～23 日最后 30 分钟的历史同日 0DTE 链缺失，不能回填或插值。
- 状态规则上线后仍需要 forward shadow 样本、成本与 walk-forward 验证，才能
  讨论阈值调整；不能用同一批日期同时选参数和宣称有效。

详细根因与证据见
`docs/rth-state-data-quality-audit-2026-07-24.md`。
