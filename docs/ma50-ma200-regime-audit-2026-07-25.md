# MA50/MA200 日内四态审计（2026-07-25）

## 技术结论

旧的“同向均线 4 笔全亏”不是反向均线 Alpha。它混合了均线滞后、追价、
生命周期重复和目标先于可执行入场四个问题：

- 9 笔原始 confirmed 样本只有 4 个交易日；去掉一笔 10 分钟内的重复经济事件，
  再去掉一笔目标在首个可执行 ask 前已到达的伪入场后，只剩 7 笔独立可执行样本。
- 两笔仍然亏损的旧“均线同向”交易，在新分类中分别是
  `TREND_EXTENDED/down`（价格距 MA50 3.58 ATR、距 MA200 8.48 ATR）和
  `REGIME_TRANSITION/up` 下的逆向 Put；它们不是健康的 `TREND_ALIGNED`。
- 15 个 RTH、1,125 个因果 5 分钟边界中，`TREND_ALIGNED` 仅 19 个边界。
  因此金叉/死叉和静态 MA50−MA200 符号不能作为 Call/Put 触发器。
- `TREND_EXTENDED` 的 30 个可评估 15 分钟 episode 只有 12 个同向为正，
  合并命中率 40.00%，平均同向变化 −2.08 ES 点。该结果支持“不追同向凸性”
  的风险提示，但样本仍不足以证明可直接做反向交易。

上线语义是只读共振：`TREND_EXTENDED` 提示禁止追价，
`REGIME_TRANSITION/MIXED` 要求等待 wall/flip 接受或拒绝，
`TREND_ALIGNED` 也只提供背景，不改变 Spring Gamma D/Q/V、墙位方向或自动下单。

## 四态覆盖

统计窗口为 2026-07-06 至 2026-07-24 的 15 个完整 RTH。每个交易日
09:45–15:55 ET 每 5 分钟评分一次，共 1,125 个边界。

| 状态 | 边界数 | 占比 | Episode 数 | 解释 |
|---|---:|---:|---:|---|
| `TREND_EXTENDED` | 389 | 34.58% | 31 | 方向统一但价格距 MA50/200 过远 |
| `MIXED` | 377 | 33.51% | 53 | 价格、排列或斜率互相冲突 |
| `REGIME_TRANSITION` | 340 | 30.22% | 27 | 价格与 MA50 已转向，慢排列尚未完成 |
| `TREND_ALIGNED` | 19 | 1.69% | 8 | 价格侧别、排列和 3/6 根斜率一致 |

四态覆盖的是环境，不是交易标签。状态边界连续分钟高度相关，因此前瞻统计优先
使用状态 episode 的首个边界，而不是把 1,125 个重叠分钟当作独立样本。

## 15 分钟 episode 结果

`directional_points` 把向下状态的 ES 变化乘以 −1，使正数统一表示状态方向继续。
命中率置信区间为二项 Wilson 95% 区间。

| 状态/方向 | n | 命中率 | 95% CI | 平均同向点数 | 中位同向点数 |
|---|---:|---:|---:|---:|---:|
| `REGIME_TRANSITION/down` | 14 | 57.14% | 32.59%–78.62% | +2.02 | +1.88 |
| `REGIME_TRANSITION/up` | 12 | 58.33% | 31.95%–80.67% | −1.60 | +1.75 |
| `TREND_ALIGNED/up` | 6 | 50.00% | 18.76%–81.24% | −3.29 | −3.38 |
| `TREND_EXTENDED/down` | 13 | 38.46% | 17.71%–64.48% | −4.38 | −3.75 |
| `TREND_EXTENDED/up` | 17 | 41.18% | 21.61%–64.00% | −0.31 | −1.50 |

置信区间都跨越 50% 附近，不能宣称单独 Alpha。最稳定的可执行结论仅是：
延伸状态不适合继续追同向 0DTE 凸性；转折状态要等真实 wall/flip 生命周期，
不能抢跑 MA50/200 交叉。

## 净化后的 7 笔 RTH confirmed 样本

该表使用 `rth_1300`、naked、exact-contract ask 入场和 13:00 ET 前策略窗口。
盈亏单位为每张合约美元，未计佣金、滑点和人工延迟。

| 决策时间 ET | 方向 | P&L | MA 状态 | MA 方向 | 距 MA50 ATR | 距 MA200 ATR | 解释 |
|---|---|---:|---|---|---:|---:|---|
| 07-14 10:32 | Up | +$1,030 | `MIXED` | - | +0.74 | −1.22 | wall 确认有效，均线不提供方向 |
| 07-15 10:54 | Down | −$520 | `MIXED` | - | +0.83 | +1.70 | 均线冲突，单样本失败 |
| 07-15 11:50 | Down | +$240 | `MIXED` | - | −0.43 | +0.93 | wall 路径胜于静态排列 |
| 07-23 09:46 | Down | +$730 | `REGIME_TRANSITION` | Down | −8.97 | −7.46 | 早盘转折继续，但已极端偏离 |
| 07-23 12:16 | Down | −$460 | `TREND_EXTENDED` | Down | −3.58 | −8.48 | 典型同向追价失败 |
| 07-24 10:07 | Up | +$460 | `TREND_EXTENDED` | Down | −0.20 | −6.46 | 逆慢趋势 wall 机会，说明不能硬 veto |
| 07-24 11:34 | Down | −$460 | `REGIME_TRANSITION` | Up | +3.56 | −0.80 | 旧死叉掩盖短期上行转折 |

7 笔合计 +$1,020、4 胜 3 负。若事后删除两笔新状态明确警告的亏损，
结果会变成 5 笔 +$1,940；但这属于同样本事后筛选，不能作为上线收益预期。
相反，硬性删除所有逆 MA 方向交易还会删除 07-24 的 +$460 winner。

## 指标与阈值

所有特征仅使用同一 ES 合约的闭合 RTH 5 分钟 bar，不插值：

- `distance_to_sma50_atr = (P − MA50) / ATR14`
- `distance_to_sma200_atr = (P − MA200) / ATR14`
- `ma50_ma200_spread_atr = (MA50 − MA200) / ATR14`
- 斜率为当前均线减 3/6 根前均线，再除以 ATR14
- ATR14 跨交易日保留有效日内 true range，但排除 GTH 和隔夜跳空
- 侧别阈值 0.10 ATR；斜率阈值 0.02 ATR
- 延伸阈值为 `|P−MA50| >= 2 ATR` 或 `|P−MA200| >= 4 ATR`
- 新鲜交叉不超过 6 根闭合 bar；两根持续性单独记录

四态都没有 `action_authority`。MA200 与最近稳定 wall/flip 相距不超过
0.50 ATR 时只标记 `decision_zone`，含义是等待该结构的接受或拒绝。

## 数据与方法

MA 状态回放来自 `scripts/build_market_state_5m_ibkr_history.py`：IBKR
`TRADES`、5 分钟、`useRTH=true`，固定 ES 2026-09 合约；每个时点只使用
`bar_end <= as_of` 的数据。窗口内 15 个完整 RTH、1,125 个边界和 119 个
MA episode，11 个 sector ETF 均为 3,276 根历史 bar。

交易净化来自 `scripts/backtest-0dte-levels.py`：

- confirmed 事件按到期交易会话、level kind、level、方向和合约执行
  10 分钟冷却；
- 首条残缺 event id 不能由未来记录补全；
- decision→首个可执行 ask 的 underlier 路径逐相邻样本限制为 30 秒；
- target 或 invalidation 先发生时，不生成伪交易；
- terminal 同结构位必须先离开 reset band，结构真正迁移或删除才允许重装。

## 限制与稳健性

- 15 个交易日和 7 笔净化交易不足以验证期权 Alpha，且状态 episode 仍可能同日相关。
- IBKR 历史 bar 可验证规则路径，但不证明当时生产投递、NBBO 和 wall 数据面完整。
- 期权盈亏未计佣金、滑点、成交概率、人工延迟和仓位规模。
- 07-20–22 的旧 lifecycle bug 使历史交易样本非随机缺失。
- SPX 等价值 MA200 是 `ES MA200 − 同步 ES−SPX basis` 坐标投影，
  不是现金 SPX 自身历史 MA200。
- 生产热状态不会跨合约续接。合约 roll 后必须等新合约 live identity 已确认，
  再使用精确合约 IBKR 历史 RTH bar 做一次受控 warm-start，绝不能把旧合约
  MA 历史拼入新合约。

## MA200 生产 warm-start

`scripts/warm_market_state_ma_history.py` 只向有界 `rth_ma_history` 注入精确
ES 合约的闭合 RTH 聚合 bar，不改 `closed_bars`、`current_bar`、源时间或
既有完整 live bar。若顶层合约身份为空，只有最近一个完整 RTH session 的每根
live bar 都确认同一合约后，才将身份提升为该精确合约。默认先 dry-run；生产
写入要求 market-feature hot worker 精确处于 `inactive`，并执行锁内二次状态
检查、固定 hot-worker 进程 owner lock、SHA compare-and-swap、owner-only
原始备份、严格目录 fsync 和原子替换。CLI 不能改写目标 service unit，live
重叠收盘差上限固定为 2.00 ES 点且不可上调。

```bash
.venv/bin/python scripts/warm_market_state_ma_history.py \
  --host 127.0.0.1 --port 4002 --client-id 299 \
  --es-expiry 20260918 --duration "1 M" \
  --min-overlap-bars 6

systemctl --user stop spx-spark-market-features-hot.service
.venv/bin/python scripts/warm_market_state_ma_history.py \
  --host 127.0.0.1 --port 4002 --client-id 299 \
  --es-expiry 20260918 --duration "1 M" \
  --min-overlap-bars 6 --apply
systemctl --user start spx-spark-market-features-hot.service
```

应用前必须满足：IBKR qualified exact contract、最近 320 根连续 RTH bar、
历史末端严格等于最近完整 session 收盘、该 session 全部 5 分钟 live bar 均为
同一精确合约且质量为 `ok`，以及所有重叠收盘差不超过 2.00 ES 点。这样历史
seed 不会补掉当日 D/Q/V 的采集缺口，partial/ambiguous live bar 仍会使 MA
fail closed。写后工具会锁内重读并验证 320 根和完整 MA `ready`；任一步失败
会原子恢复原字节。返回的 `backup_path` 保留本次迁移前的可恢复状态。

## 推荐动作

1. 将四态作为 15 分钟报告和 Radar 的只读背景立即上线，不改变自动执行。
2. 明确执行纪律：`TREND_EXTENDED` 不追同向凸性；
   `REGIME_TRANSITION/MIXED` 必须等待 wall/flip 接受或拒绝。
3. 每日保存 state×wall lifecycle×D/Q/V×NBBO 的决策时点标签；
   累计至少 20 个独立 episode/方向后再评估是否升级为软 gate。
4. 对候选 gate 做 walk-forward：先冻结阈值，再比较净收益、MFE/MAE、
   成交成本和被删除 winner，不以同样本最优参数晋升。

## 待回答问题

- `TREND_EXTENDED` 的警告应主要由 MA50 距离、MA200 距离还是二者交互触发？
- 转折方向与 wall 确认相反时，何种 D/Q/V 与宽度组合仍值得保留？
- 1DTE 卖方表达和 0DTE 买方表达是否需要不同的延伸阈值？

### 图表映射与复现说明

- 状态覆盖：水平 bar；字段为 state、slot count、share、episode count。
- 15 分钟前瞻：有正负零线的水平 bar；字段为 state/direction、
  mean directional points、median、hit rate、n 和 Wilson interval。
- 交易表：7 笔净化 exact-contract 样本，按决策时间升序。
- 所有显示值来自 2026-07-25 的两次可复现命令输出；原始 NBBO 不插值。
