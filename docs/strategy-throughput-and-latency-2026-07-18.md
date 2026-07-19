# SPX 0DTE：每日 1–2 笔目标的门禁与实时性诊断

## Executive Summary

结论不是“门禁整体太严”，而是两类问题叠加：历史执行链路确实过慢；数据/候选采集失败又被混在策略拒绝里。2026-07-14 至 2026-07-17 四个健康完整 session 的 RTH 漏斗为：4 个 confirmed → 3 个 material intent → 2 个 trade-ready → 2 个 fresh exact L1 → 1 个 strict replay fill。严格回放成交产出仅 0.25 笔/session，距离每天 1–2 笔明显不足，但其中一个 RTH confirmed 根本没有进入 intent，因此不能归因于硬门禁。

7 月 14–15 日 RTH `market_features` 中位启动间隔约 62 秒；7 月 16–17 日已改善为约 5 秒。两个真实 trade-ready 都发生在旧 cadence 下，从确认到决策分别耗时 82.38 秒和 47.45 秒，而决策时 quote age 只有 1.86 秒和 0.73 秒：行情本身新鲜，慢的是确认后的处理链。新 5 秒 cadence 尚无新的 trade-ready 样本可做端到端验证；当前仍是轮询式决策/告警和虚拟执行，不是独立的实时 broker execution lane。

每日 1–2 笔不应变成强制配额。更安全的运营目标是平均每周 5–10 个合格操作、每天最多 2 个、允许无交易日，并把 valid-signal capture 提升到至少 90%。

## Key Findings

1. 四个健康完整 session 有 14 个独立 confirmed 事件，候选供给并不为零；RTH 是 4 个，GTH 是 10 个。
2. RTH 只有 3/4 进入 material intent，2/4 成为 trade-ready；严格回放只有 1 个成交，另一个在限价满足前先达到目标。
3. GTH dip/reclaim 有 6 个 legacy 信号，但 forward-v3 exact two-leg entry 为 0，不能据此判断 5 秒门禁的收益效果。
4. 20-session readiness 是离线研究晋级门，不会阻挡生产信号或 trade intent；它不是低交易频率的原因。
5. `option_structure_not_ready`、`option_l1_not_ready`、候选缺失等应该标为可重试的 `pending_data`，不能混算成策略拒绝。
6. schema、policy、coordinate、expiry、TTL、exact NBBO、quote freshness/skew、max loss 与语义去重属于安全门，应继续 fail closed。
7. 唯一有真实 RTH 样本的软策略限制是 `es_return_1m_points_opposes_direction`；样本只有 1 个，适合 shadow counterfactual，不适合现在直接删除。

## Recommended Next Steps

### P0：先把执行链改成热路径

1. detector/intent/quote/action 先做确定性校验并持久化，再通过 async outbox 执行 LLM 解释与 Bark/飞书通知。
2. action 前重新读取真实 wall clock 和 quote；禁止复用服务开头捕获的旧 `evaluation_now`。
3. 为交易热路径使用独立 worker，不与 15–30 秒甚至更慢的 realtime/notification 任务共享拥塞。
4. GTH dip/reclaim pending 阶段预订阅计划 long/short 两腿，在 signal TTL 内保持 hot lane；不要靠放宽 stale quote 门禁补数据。
5. 持久化 detector→intent、intent→quote、quote→persist、persist→outbox、outbox→deliver 的分段耗时和唯一终态 blocker。

### P1：把门禁分层并验证软门

1. `pending_data`：结构、候选、行情暂缺；TTL 内重试。
2. `rejected_strategy`：方向、regime、确认评分未达标；记录 counterfactual。
3. `blocked_safety`：契约、TTL、expiry、coordinate、quote freshness、最大风险失败；继续硬拦截。
4. 对 5/10/15 秒 freshness、15/30 秒 entry window、ES 1m opposition 与冗余微观确认做 shadow 对照；固定 cutoff 后按同一 cohort 比较。
5. 积累至少 20 个 forward complete sessions、20 个 GTH exact entries 和 20 个 Put exact entries，再讨论经济性晋级。

## Operating Targets

| 指标 | 建议目标 | 当前证据 |
| --- | --- | --- |
| 合格操作 | 平均 5–10/周；最多 2/日；允许 0 | strict replay 1/4 session |
| valid-signal capture | ≥90% | RTH material intent 3/4 |
| detector→intent | p95 <1 秒（不含明确 hold） | ready 事件 47.45/82.38 秒 |
| fresh quote→persist | p95 <0.5 秒 | 尚未分段埋点 |
| RTH 热路径 cadence | p95 gap ≤5.5 秒，不能 >10 秒 | 7/17 p95 6.10 秒；2 个 gap >10 秒；max 13.02 秒 |
| GTH exact-spread evidence | ≥20 后再晋级 | 0 |

## Caveats

生产 trade-ready 只有 2 个，strict replay fill 只有 1 个；不能声称策略已有正 edge，也不能声称放松门禁会改善收益。旧 GTH virtual open 缺 provider/source timestamp/bid/ask/exact spread legs，不能证明真实可成交。Top-of-book 回放没有队列、滑点、部分成交、佣金或人工反应延迟。

本报告用于工程诊断和研究裁决，不构成投资建议；`automatic_ordering=False`。

## Further Questions

- 修复热路径后，confirmed→material intent capture 能否稳定达到 90%？
- GTH 两腿预热后，5 秒 exact freshness 的同腿同步覆盖率是多少？
- 20 个 forward session 后，软策略拒绝在同一 exact-spread cohort 中贡献了多少收益/回撤差异？
- broker order/fill 状态机何时进入端到端 SLO，而不再只验证 virtual execution？
