# 策略、K3 改动与缺口复盘（第三次修正版，2026-07-18）

## 结论先行

K3 的大部分工程改动方向是对的，但旧回测把观察性 S2 当成 production gate，又错误处理
`chain_implied_spx` 坐标，所以“GTH 很差、Put/down 很差、baseline -$2,155”的归因不成立。

本轮进一步完成了六项修正：

1. 回测直接读取持久化的 `trade_ready`，严格重放 provider、contract、limit、entry window、
   target 与 invalidation。
2. S2 改回 observation-only，按 semantic key 去重，并按 official SPX、chain parity、raw ES
   三种原生坐标 fail closed。
3. GTH 增加 signal TTL、suppression/provider reset 和 exact-spread executable NBBO 门。
4. GTH trend alignment 仅以版本化 shadow hypothesis 采集，未用 6 个历史样本过拟合上线。
5. GTH signal、RTH trade intent、confirmed gate 与 virtual lifecycle 统一为 v3 五字段契约：
   `schema_version + policy_version + valid_until + coordinate + block_reasons`。
6. 增加 forward-only readiness 门：旧 v1/v2 永久排除，只有自然产生的 v3 完整 session 和
   point-in-time exact quote/spread 才能进入下一次参数裁决。

固定 cutoff 2026-07-17 的真实 production 证据只有 **2 个 trade-ready、1 个模拟成交、
+$800**；另一个 Put 在限价满足前已经到目标，因此不入场。这个结果修正了语义，但样本太小，
不能证明策略或 Call 优于 Put。当前 forward-v3 裁决进度为 **0/20**；截止日虽然有 5 个
observed partitions，但只有 2026-07-14..17 四个 health-complete session，且都早于 forward
policy window。2026-07-13 因 GTH 覆盖只有 85.19% 而不完整；43 条 legacy material
decision、474 次重复 evaluation 和 6 个 legacy GTH 均不得回填。
`automatic_ordering=False` 必须保持。

详细方法、时序、表格和网站数据见
`docs/strategy-backtest-validation-2026-07-18.md`。

## 1. 对 K3 改动的审阅判断

| 范围 | 判断 | 本轮补充/修正 |
|---|---|---|
| GTH DST/09:45 ET/expiry cap | 正确，可保留 | 继续测试夏冬令时和不跨 expiry |
| GTH exact debit-spread shadow | 方向正确 | 再要求双腿有序 NBBO、debit ask<width、signal 未过期 |
| S2 follow-through | 可作观察特征 | 不等于 production gate，不得汇总成策略 PnL |
| S3 recorded spread | 正确 | legacy 无 exact spread 继续 fail closed |
| production trade intent | 现有门较完整 | 回测已改读真实 trade-ready，不再用 S1/S2 替代 |
| Put/down 方向 | 实现正确 | 不硬禁；按 session×direction 固定分层 |
| GTH trend filter | 有研究价值 | 默认 shadow；通过 forward-only readiness 后才裁决 enforce |
| 自动下单 | 证据不足 | 继续关闭 |

## 2. 旧负值为什么不能再用

### S2 不是 production `trade_ready`

旧 S2 baseline 有 12 笔，但 12/12 都只来自 pricing-outcome touched + 15 秒公式；0/12 曾
进入 confirmed/formal/actionable/trade-ready。生产还要求市场锚、方向一致性、repricing、
报价 freshness、target room、reward/risk 等门。

### 坐标错误与 GTH/Put 混杂

旧 loader 把所有 `kind != official_spx` 都当成 `future:ES - 45`：

- `chain_implied_spx` 实际是 `synthetic:SPXW_PARITY`，不能替换成 ES 或 official SPX。
- `es_equivalent` 应使用记录的 raw ES `trigger_target`，不能固定减 45。

旧 8 个 S2 Put 中 5 个属于 GTH/down；这些错误 cohort 合计贡献了主要负值。因此方向映射
虽然 267/267 一致，汇总表却把坐标问题误写成了 Put 问题。

### 重复与陈旧特征

267 个 touched rows 按 semantic key 只有 233 个机会；旧 baseline 还重复计算过同一经济路径。
session/trend/vol 多在 seed 时冻结，实际 touch 晚到时不能拿来样本内优化。

## 3. 本轮真实 production 回测

### Intent coverage

| status | records | distinct events | 语义 |
|---|---:|---:|---|
| observing | 452 | 279 | 非终态，不算 pass/block |
| blocked | 20 | 5 | 明确阻断 |
| trade_ready | 2 | 2 | strict replay cohort |

### 两个 trade-ready

| intent | 结果 |
|---|---|
| 2026-07-14 7530C breakout | ask 19.8 达到 limit 19.9；baseline 模拟 +$800 |
| 2026-07-15 7560P breakout | 15:51:04 先到目标；15:51:25 才满足 limit，跳过无 PnL |

因此不能说 Put 亏，也不能说 Call 已证明有效。正确结论是：limit-window 回放必须把
`target_before_entry` 当作未成交，而当前 production cohort 远不足以比较方向。

## 4. GTH 实际修复

历史 6 个 GTH confirmations 都建立过 virtual episode，其中 2 个分别延迟 826 秒和 5,261 秒，
超过 600 秒 TTL。原因是 signal file 持续存在，而 virtual entry 只校验 session/expiry。

现在：

- signal 使用 v3 五字段契约，policy 由有效配置生成稳定指纹；
- `valid_until` 采用 exclusive boundary，`now >= valid_until` 一律过期；
- GTH trigger coordinate 明确为 raw ES / `future:ES`，不再与 SPX 或 ES-equivalent 混用；
- 成功事件保持 `block_reasons=[]`，阻断事件持久化稳定、可审计的原因列表；
- missing/invalid/expired/future>5s 的信号不能开 episode；
- macro/active suppression 清空 pending，解除后重新确认；
- provider switch 重置 pending；
- exact spread 要求同 expiry/right、两腿新鲜同步、有序 NBBO、`0 < ask < width`；
- entry 保存 bid/mid/ask 与 signal age；
- trend regime 只记录 shadow verdict，不改变告警或自动执行。

这些是可发布的确定性安全修复，但不是 GTH edge 的证明。

## 5. 修正后的 S2 只作观察

| 指标 | 旧实现 | 修正后 |
|---|---:|---:|
| touched rows/opportunities | 267 | 233 observed unique；217 health-complete |
| baseline executable | 12 | 6 |
| total PnL | -$1,560 | +$100 |

从负变正说明坐标、重复和 session 健康筛选足以翻转小样本结果；因此两边都不能作为上线
依据。S2 继续用于监控 follow-through 与数据覆盖，不进入 production strategy total。

## 6. v3 五字段契约已经统一，但旧数据绝不升级身份

| 字段 | 统一语义 |
|---|---|
| `schema_version` | 新决策链固定为 v3；state 文件的内部 schema 独立管理 |
| `policy_version` | 标识真正参与决策的规则和有效配置；不同 producer 各自版本化 |
| `valid_until` | 当前事件允许下一步动作的最后边界，统一使用半开区间 |
| `coordinate` | 冻结原始触发坐标；official SPX、chain parity、ES-equivalent、raw ES 不互换 |
| `block_reasons` | 始终为稳定去重列表；成功为空，失败必须说明原因 |

迁移是 **forward-only**：v1/v2 记录继续保留用于错误审计和历史诊断，但不能补字段后冒充
v3，也不能进入 20-session readiness 的分子或分母。否则会把事后可见信息写回历史，重新制造
look-ahead 和 schema 漂移。

## 7. 冻结的 20-session / exact-data 裁决门

截至本报告快照，forward-v3 合格证据全部为 **0/20**。这里的 20 是不同的完整交易 session
或不同的终局 entry/exit episode，不是服务每几秒重复生成的 evaluation row。

| 裁决门 | 当前 | 目标 | 未达门槛时的强制状态 |
|---|---:|---:|---|
| contract-consistent 完整 session | 0 | 20 | collecting |
| GTH exact two-leg entry | 0 | 20 | GTH entry/trend 仅 shadow |
| Put point-in-time exact entry | 0 | 20 | Put 规则仅 shadow，不禁用也不晋级 |
| exact-spread 完整 entry→exit | 0 | 20 | sat85/trail33/clock 仅 shadow |

真实 cutoff 2026-07-17 共观察到 5 个 feature partition，其中 2026-07-14..17 四个通过健康
完整门。2026-07-13 的 GTH 分钟覆盖为 673/790，即 **85.19%**，低于 90% 门槛，因此不能写成
complete session。四个健康完整 session 又都不在 forward policy window，所以
contract-consistent 仍为 0/20。另有 43 条 legacy material decision 被显式排除：confirmed
gate 1、GTH signal 6、trade intent 22、virtual strategy 14。

完整 session 还要求 GTH 与 RTH 各自分钟覆盖达到预注册门槛；所有 forward 事件必须满足 v3
契约且无重复 semantic sample。达到 20 只意味着 **ready for review**，不会自动晋级。裁决仍需
固定 cutoff、固定 policy cohort、按 GTH/RTH × direction × thesis 分层并人工审阅。

## 8. 发布边界与剩余缺口

| 项目 | 当前决定 |
|---|---|
| GTH TTL/suppression/provider/NBBO | 合并后部署 |
| GTH bullish alignment | collecting / shadow only，未达门不得晋级 |
| RTH Put | collecting / shadow only，不硬禁、不晋级 |
| GTH/coordinate-unavailable proxy | fail closed |
| sat85/trail33/clock 参数 | collecting / shadow only，不推广 |
| automatic ordering | 继续关闭 |

仍需：20 个 contract-consistent 完整 session、20 个自然产生的 exact GTH entries、20 个 Put
exact entries、20 个 exact-spread 完整 exits、真实 entry window/fill-time NBBO、固定的
RTH/GTH × direction × thesis walk-forward，以及非随机缺失审计。

## 9. 证据与复现

- 主回测：`/srv/data/spx-spark/data/reports/odte_level_backtest/validation=2026-07-18-contract-v3/cutoff=2026-07-17/`
- 前一 cutoff：`/srv/data/spx-spark/data/reports/odte_level_backtest/validation=2026-07-18-contract-v3/cutoff=2026-07-16/`
- 完整验证：`1541 passed`，1 条第三方 deprecation warning
- 技术报告：`docs/strategy-backtest-validation-2026-07-18.md`
- 诊断 notebook：`docs/notebooks/entry-quality-gates-2026-07-18.ipynb`
- 网站 artifact：`docs/strategy-backtest-validation-2026-07-18.artifact.json`
- readiness 实现：`src/spx_spark/data_platform/research/strategy_readiness.py`
