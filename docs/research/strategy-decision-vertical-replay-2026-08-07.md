# Strategy Decision v2 · S2/S3 Replay · 2026-08-07

状态：**Bootstrap engineering gate passed；不得表述为已验证 edge；生产 owner 尚未切换。**

## 数据与因果范围

- 权威数据：Oracle `/srv/data/spx-spark/data`，代码基线 `5aa4805`。
- 覆盖：2026-07-14 至 2026-08-06，共 18 个完整 session。
- RTH：17 个 confirmed fade/reclaim 事件；11 个具有可重放的两腿 ask 入场、bid 退出。
- GTH：2 个旧 Manual Ready；1 个具有完整退出报价，另一笔因退出腿报价缺失而删失。
- ATR5m 来自同 event 的最近 trade-intent 事实；GTH 两笔最近记录相差小于 1.7 秒。
- 每个输入满足 `available_at <= decision_at`，未发现 lookahead violation。
- 机会收益不是 broker fill：入场使用 conservative synthetic ask，退出使用 synthetic bid。

## S2 决策结果

| 项 | 数量 |
|---|---:|
| 可比较双腿机会 | 12 |
| 新规则 `TRADE` | 3 |
| 新规则 `NO_TRADE` | 16 |
| `direction_valid_but_entry_too_late` | 5 |
| `stop_distance_outside_atr_band` | 5 |
| 历史双腿报价未达到/不完整 | 6 |

两笔 2026-08-06 GTH Call 卡均被新规则拒绝：止损距离分别为 2.88 ATR 与 3.16 ATR，超过 1.0 ATR 上限。第一笔没有可用退出腿报价；第二笔 gross 约为 0，在参考成本下为 -$25。

## Legacy vs New

单位：每组 SPXW Vertical 的美元净收益；滑点为每腿每侧期权点。

| 滑点 | Legacy Net PnL | New Net PnL | Legacy ES10 | New ES10 |
|---:|---:|---:|---:|---:|
| 0.00 | $750 | $1,250 | -$220 | $0 |
| 0.05 | $510 | $1,190 | -$240 | $0 |
| 0.10 | $270 | $1,130 | -$260 | $0 |
| 0.20 | -$210 | $1,010 | -$300 | -$5 |

参考成本 0.05 下，被 Late Chase 门拒绝的 Legacy 亏损合计为 -$695。

## 必须保留的限制

1. 历史事件没有完整保存 `distance_to_vwap` 与 `impulse_15m`，本次按中性值处理，因此 New 结果偏乐观；上线实时路径必须使用真实字段。
2. RTH 合约来自既有 wall-spread quote replay，不等同于生产时 `call/put_skew_spread_shadow` 一定会选择同一双腿。
3. 样本只有 12 个可比较机会、5 个候选 session；不能据此宣称统计 edge。
4. 2026-08-05 与 2026-08-06 的 frozen case 已完成，但只有两个预先冻结的判别案例，不是独立样本。
5. `NO_TRADE` 按零收益计；未估计放弃机会成本。

## S3 Oracle 冻结案例原始输出

以下结果只使用各决策时点或更早的 Oracle Parquet/NBBO；正式收盘价仅用于验收描述，不进入特征。

```json
{"session":"2026-08-05","decision_at":"19:59Z","spx":7732.72,"vc15_spx":7736.65,"vc30_spx":7737.36,"vc60_spx":7738.68,"er30":0.2432,"q_mode":7730.0,"atm_straddle_decay_15m":-0.0123,"terminal_state":"PIN_MIGRATING"}
{"session":"2026-08-06","decision_at":"19:00Z","spx":7712.94,"vc15_spx":7712.56,"vc30_spx":7712.69,"vc60_spx":7714.18,"er30":0.1429,"q_mode":7710.0,"atm_straddle_decay_15m":0.0448,"terminal_state":"PIN_STABLE","top_center":7710.0}
```

8 月 6 日 19:00Z 的 Schwab 三腿原始 BBO：

```json
{"contract":"20260806 7700C","bid":15.1,"ask":15.3}
{"contract":"20260806 7710C","bid":7.3,"ask":7.5,"quantity":-2}
{"contract":"20260806 7720C","bid":2.5,"ask":2.6}
{"synthetic_bid":2.60,"synthetic_ask":3.30,"mid_pricing_used":false}
```

8 月 5 日最后 15 分钟 7740 固定 straddle 从 6.10 升到 6.175，且末段价格接受新低，因此 7740 Butterfly 被禁止。8 月 6 日 7710 固定 straddle 从 11.15 降到 10.65；一分钟路径在 7715 附近出现两次 excursion-return，Q mode 与 value center 同时支持 7710 进入 Top 1。宽翼没有在缺少更优 conservative BBO 时获得优先级。

## Bootstrap Gate

| 检查 | 状态 |
|---|---|
| no-lookahead | PASS |
| 至少 15 个完整 session | PASS（18） |
| 至少两项改善 | PASS（Net PnL、ES、Late-Chase loss） |
| Manual Candidate 合同完整 | PASS（实现与构造测试） |
| `automatic_ordering=false` | PASS |
| 2026-08-05 frozen case | PASS（PIN_MIGRATING；禁止 7740 Butterfly） |
| 2026-08-06 frozen case | PASS（PIN_STABLE；7710 Top 1；三腿 BBO） |

结论：`bootstrap_gate=pass`（工程 bootstrap，不是 edge 证明）。代码可进入 S4；旧 Vertical/GTH green-card 的生产 owner 仍须等合并、周末 cutover 和真实消息验收后才能删除或降级。
