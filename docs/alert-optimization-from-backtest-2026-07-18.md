# 告警优化建议（修正版，2026-07-18）

本文件替代同日早先基于含前视/陈旧报价回测写出的建议。完整证据和缺口见
`docs/strategy-review-and-gaps-2026-07-18.md`。

## 当前应保留的改动

1. `trade_ready` 的 follow-through 继续使用
   `方向×(spot-trigger) >= max(2, 0.05×EM)`，并在完整 15 秒 hold 之后重取期权 ask。
   当前证据不支持放宽；也不再宣称它已经提高胜率。
2. GTH 精确腿只在 structure/session/expiry/basis/quality 全部新鲜一致时展示；否则退回
   非执行观察，等待新鲜 SPXW NBBO。
3. GTH 退出固定为到期日 09:45 America/New_York，自动处理 DST，绝不滚到次日。
4. GTH virtual shadow 跟踪 exact debit spread 双腿净值与净 Greeks，双腿 age/skew≤5 秒；
   sat85 只作为 shadow exit，不作为已验证的优胜规则。
5. 自动下单继续关闭，数量继续人工确认。

## 修复后证据

| 项目 | 修复后结果 | 告警含义 |
|---|---|---|
| S2 production gate | 267 信号中 12 可执行；33% 胜率，平均 -$130 | 门后样本仍弱；不得把 observation 当入场 |
| S1 confirmed | 14 中 8 有报价；38%，平均 -$19 | 不是 `trade_ready`，只作 FSM 研究集合 |
| S3 gth_dip | 6 中 3 可执行；3/3 亏，平均 -$147 | GTH 入场仍未验证 |
| S3 exact spread | 历史 6 个事件均未保存 spread；严格 n=0 | 无法裁决 production sat85 |
| 账单核心 cohort | 严格 common 1/7；sat85=trail33=clock | 不再宣称 sat85 赢过 clock/actual |
| 最新一日 | 新增 S2 无可执行；新增 S3 可执行交易为负 | 没有正向 holdout 证据 |

## 告警分层

### RTH

- `observation`：只描述关键位、方向条件和 invalidation；不得出现“已确认买入”。
- `trade_ready`：只有生产全部门控通过、门后报价≤5 秒且 reward/risk 有效时发送。
- play stats：样本不足时整块省略；出现时 LLM 必须保留 play、level_kind、n 和胜率。

### GTH

- ES dip-reclaim 可以作为背景观察。
- 没有同 session 的新鲜结构/basis 时，不给具体 long/short 行权价。
- 有 exact spread 时仍须提示“新鲜 NBBO 后人工确认”，且 virtual shadow 只接受两腿都新鲜、
  同步、质量为 ok 的组合。
- 09:45 ET 之后不再创建当前 0DTE episode。

## 暂不实施

- 不把 wide invalidation 上生产：平均结果仍为负，且没有 OOS。
- 不用 trailing 替换 RTH 固定退出：修复后均值从 -$94 变为 -$113。
- 不把 sat85 写成推荐优于 clock：核心 GTH Call cohort 在 30/5、90/15、300/60 三档
  报价门下均与 clock 相同。
- 不用 S1 confirmed 的 RTH 小切片（n=4）直接生成新过滤器。

## 下一次裁决门槛

下一次参数决策前应同时满足：

1. 至少 20 个完整交易 session，规则预先冻结；
2. RTH/GTH 分层报告；
3. 直接使用持久化 `trade_ready`，不以 S1 confirmed 代替；
4. S3 必须有保存的 production exact spread；
5. 主账单回放报价门固定为 entry/mark 30 秒、leg skew 5 秒，common 覆盖率单独报告；
6. 日期 walk-forward/holdout 与累计结果并列，缺失报价记为 missing，不记为收益 0。
