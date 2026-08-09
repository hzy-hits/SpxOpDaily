# SPX Spark 0DTE 策略信号引擎 v3 P1/P2 设计合同

状态：**已批准**（2026-08-09 用户确认「听你的 你去落实设计文档和实现文档吧」，
采纳方案 A：候选级持久化复用现有三张表，零新表）。

本文是 `docs/strategy-signal-engine-v3.md` 之上的增量合同，不整体替代 v3。
除本文明确修订的条款外，v2（`docs/strategy-signal-engine-v2.md`）与 v3 的全部
约束继续有效。执行任务卡见
`docs/strategy-signal-engine-v3-p1p2-execution.md`。

## 0. 本文解决什么问题

v3 P0 三件套（flood-control 自我阻断、directional butterfly 降权、排序键降级，
master `bcb9a685`）合入后，用户审查将 V3 状态标定为：

> V3 architecture accepted · shadow/research ready · manual delivery blocked
> pending P0 fixes（已修）· edge unvalidated

edge 验证被以下五个 P1 缺口阻塞，外加一个 P2 增强：

1. **只有赢家留痕**：`persist_strategy_decision` 只持久化 rank 1 候选的冻结腿。
   rank 2/3 候选没有腿、没有多 horizon 标记，「排序器选对了吗」这一 edge 验证
   的核心问题在数据上不可回答。
2. **没有结构失效标签**：`observe_due_strategy_outcomes` 是纯时间 horizon 标记
   （1–20 分钟）。SPX 早已穿过 `invalidation_spx` 的样本与健康持有样本混在
   同一桶里，ManagementPolicy 校准被污染。
3. **删失样本静默丢失**：退出报价不可得时状态是 `exit_quote_unavailable`；
   采样延迟超过 `maximum_lag_seconds` 的观察点被 `read_due_strategy_observations`
   永久跳过，不留任何行。聚合统计把删失当缺失，存活偏差不可审计。
4. **机会级重复计数**：同一 opportunity 跨 tick 反复生成新 `decision_id`，
   `decisions.event_key` 恒为 `NULL`。聚合统计把同一机会数多次，样本量虚高。
5. **ManagementPolicy EV 只存在于离线**：`strategy_v3_freeze_acceptance.py`
   的 EV 标签只出现在验收报告里，线上候选卡看不到任何按真实管理规则
   （+50% arm / -50% premium stop / 20m time stop）估计的期望值参考。
6. **（P2）候选卡缺少结构化 thesis**：人工决策者只看到合约、价格与门审计，
   没有一段受约束的「假设 + 反证条件」帮助快速判断该不该跟。

## 1. 决策记录（Decision Record）

### D1 候选级持久化：方案 A（复用 `decisions` 表），拒绝方案 B（两张新表）

**已批准的方案 A**：rank 2/3 候选各写一行现有 `decisions` 表，
`status="shadow_candidate"`，`decision_id = f"{父decision_id}:cand{rank}"`，
冻结腿写现有 `decision_legs`，多 horizon 标记复用现有 `outcomes`。

理由：

- `read_due_strategy_observations` 只按 `strategy_name + has_legs` 过滤，
  不看 `status`——shadow 行自动进入现有多 horizon 标记管道，标记侧零改动。
- flood control（`recent_selected_strategy_cards`）只查 `status == "selected"`，
  不受影响。
- 唯一需要防污染的读路径是 `read_strategy_decisions`（replay 用），加
  `status IN ('selected','no_trade')` 过滤即可。
- 复杂度预算：0 新表、0 迁移、0 新服务。

**被拒绝的方案 B**：新增 `decision_candidates` + `candidate_legs` 两张表。
语义更干净，但需要新 alembic 迁移，并把 outcomes 读写管道复制一份以支持第二种
主体，净增约 200–300 行。在 shadow 行语义可以用 `status` 列清晰区分的前提下，
该成本不被接受。

### D2 结构失效：前向热路径只打标，不定价

前向标记（`strategy_outcomes.py`，运行在 market-features tick 内）只用已落盘的
SPX 分钟样本判断「决策后是否触及失效价、何时触及」，产出
`invalidation_breached` / `breach_at` 标签。**breach 时点的期权腿定价不进热路径**：
历史期权报价重放属于离线回填（`strategy_policy_backfill.py` 已具备 quote lake
重放能力），breach 时点的 policy P&L 在回填侧计算。理由：热路径读 quote lake
会引入不可控 IO 延迟，且前向标签只需要「是否失效」这一事实即可满足删失和
校准分桶需求。

### D3 删失是显式标签，不是缺失

新增删失状态 `censored`，携带 `censor_kind ∈ {quote_gap, session_end_before_horizon,
service_gap, breach_quote_unavailable}`。历史行里已有的 `exit_quote_unavailable`
不迁移、不回填；研究侧聚合把它映射为 `censored(quote_gap)` 读取。生产代码不加
兼容 shim。

### D4 机会身份落在现有 `events` 表

`decisions.event_key` 有指向 `events.event_key` 的外键，因此机会身份写入分两步、
同一事务：先幂等插入 `events` 行（`event_type="strategy_opportunity"`，
`event_key = f"strategy-opportunity:{session_date}:{opportunity_id}"`），再把该
key 写进 decision 行。不新建 opportunity 表。

### D5 EV 是版本化只读工件，不是配置、不是新库

ManagementPolicy EV 的线上形态是一份由回填脚本产出的版本化 JSON 工件
（`policy_ev_table.v1.json`，按 setup_kind × direction × regime 分桶），ranker
只读加载、只作展示注释。它不进 `runtime.yaml`、不进 AppSettings、不进数据库、
不进 `contracts/golden/`。样本不足的桶显式输出 `policy_ev=null` 加原因，
不允许静默外推。

### D6 Idea Memo 是 bounded LLM，同步生成、失败即省略

沿用 `call_hypothesis_critic` 的既有模式（provider client 直连 + JSON mode +
事实引用白名单校验），不引入 LangChain/LangGraph、不新增 service/queue。
memo 只在 `action_authority == "manual"`（每个交易日至多个位数次）时同步生成，
硬超时后整段省略，卡片其余部分照常投递。

## 2. 语义规范

### 2.1 Shadow 候选持久化（P1-1）

- 来源：`build_strategy_decision` 内 `rank.passed[1:3]`（最多 2 行）。
- 只持久化腿完整、报价双边就绪的候选；`_leg_rows` 校验失败的候选跳过并在
  decision payload 的 `shadow_candidates_skipped` 里留原因。
- shadow 行的 `attributes_json` 是候选级合成 payload（父 decision_id、rank、
  候选全量字段、`policy_version`、时间戳），不重复存整个父决策。
- 因果不变式与父决策一致：`available_at <= decision_at`，腿报价
  `source_at <= available_at`。
- shadow 行不投递、不参与 flood control、不出现在 replay 的决策序列里；
  它唯一的消费者是 outcomes 标记管道与研究聚合。

### 2.2 结构失效标签（P1-2）

- 判定窗口：`decision_at → target_at`（每个 horizon 独立判定）。
- 数据源：`data_root/features` 下 SPX 分钟样本（与
  `physical_followthrough.estimate_physical_terminal_range` 同一数据族）。
- 触发规则：`direction == "UP"` 时分钟低点 `<= invalidation_spx` 即 breach；
  `DOWN` 时分钟高点 `>= invalidation_spx`；NEUTRAL（蝴蝶）用候选自身的
  `invalidation_spx` 同规则判定。
- 标签字段（outcome `attributes`）：`invalidation_breached: bool`、
  `breach_at: iso8601 | null`、`label_kind ∈ {horizon_mark, structural_exit}`。
- 分钟样本在窗口内覆盖率不足（缺口超过 2 个连续分钟）时不武断判定，
  记 `invalidation_breached=null` 加 `breach_scan_gap=true`。

### 2.3 删失标签（P1-3）

| censor_kind | 触发条件 | 产生位置 |
|---|---|---|
| `quote_gap` | 到点采样但任一腿无新鲜双边报价 | `_observe` |
| `session_end_before_horizon` | `target_at` 晚于当日 16:00 ET 收盘 | `read_due_strategy_observations` 判定后由 `_observe` 落标 |
| `service_gap` | `sampled_at - target_at > maximum_lag_seconds`（原先被永久跳过的观察点） | `read_due_strategy_observations` 改为返回并落删失标 |
| `breach_quote_unavailable` | 已判定 breach 但 breach 时点无法给出保守退出参考（前向恒为此值，定价留给回填） | `_observe` |

- `status` 取值收敛为 `{observed, censored}`；`censored` 行必须带 `censor_kind`。
- `service_gap` 删失行的 `spx_return_bps` / `option_return_bps` 一律为 `NULL`，
  不允许用陈旧报价补数。

### 2.4 机会级去重（P1-4）

- 机会身份：`strategy-opportunity:{session_date}:{opportunity_id}`。
- 写入时机：`persist_strategy_decision` 事务内，selected 决策与 shadow 行都挂
  同一 event_key。
- NO_TRADE 决策不挂机会身份（没有 opportunity）。
- 聚合规则（研究侧）：每 event_key 只计首个被 outbox 接受的决策；同 event_key
  的后续决策在统计里标 `duplicate_of` 而不删行。

### 2.5 ManagementPolicy EV rank-only（P1-5）

- 工件 schema（`policy_ev_table.v1.json`）：
  `{"schema_version": "policy_ev_table.v1", "management_policy_version":
  "management_policy.v1", "generated_at": ..., "source_sessions": [...],
  "buckets": {"{setup_kind}|{direction}|{regime_terminal_state}":
  {"n": int, "ev_points": float, "p25": float, "p75": float}}}`。
- 生效门槛：桶内 `n >= 20` 才输出数值；否则 `policy_ev=null`、
  `policy_ev_reason="low_sample"`。
- 附着位置：候选 `edge` 字典新增 `policy_ev` / `policy_ev_n` /
  `policy_ev_version`；卡片文本 Edge 行追加展示。
- **硬边界**：排序键仍为 `selection_score` 优先、P/Q utility tie-break；
  policy_ev 不参与排序、不进硬门、不改变 `promotion_ready=false`。升门条件
  仍按 v3 §7.4，需用户再批准。
- 工件缺失或版本不匹配时：`policy_ev_reason="table_unavailable"`，其余照常。

### 2.6 GPT Strategy Idea Memo（P2）

- 输入：当轮 `strategy_decision` payload（含候选、门审计、market_facts 摘要）。
- 输出 schema：`{"thesis": str, "falsification": [str], "watch_levels": [float],
  "risks": [str]}`。
- 校验（不过即整段省略）：
  1. `watch_levels` 里每个数值必须能在 decision payload 的数值集合里精确找到；
  2. 全文不得出现下单指令词（市价、立即、加仓、all-in 及英文对应词表）；
  3. 不得出现 payload 中不存在的概率、价格或合约代码；
  4. thesis 与 falsification 合计长度上限 600 字符。
- 触发条件：仅 `action_authority == "manual"`；硬超时 6 秒；失败/超时/校验不过
  均静默省略，卡片主体不受影响。
- memo 不写入 `decisions` 表、不进 golden contracts、不影响任何 gate 或
  authority；渲染为卡片末尾 `Idea Memo (research)` 可选段。

## 3. 不做什么（边界重申）

1. 不新增 service、timer、数据库、队列、状态机、Rust contract。
2. 不把 EV 升为硬门；不把 memo 或 EV 写进排序键。
3. 不迁移/回填历史 `exit_quote_unavailable` 行。
4. 不在热路径读 Schwab quote lake。
5. `strategy_decision` 及其衍生字段继续不进 `contracts/golden/`。
6. `automatic_ordering=false` 不变；候选卡继续走现有 `trade_ready` lane。

## 4. 验收总门

1. 冻结回放 2026-08-05 与 2026-08-06：decision 输出（decision_id、decision_type、
   候选内容、投递结果）与改动前逐字段一致——持久化与注释类改动不得改变决策。
2. shadow 行、删失标、breach 标、event_key 在一个真实 RTH 会话内实际落盘，
   用 SQL 抽查核验（见执行方案任务卡验收段）。
3. 全量 `uv run pytest -q`、`uv run ruff check src tests scripts`、
   `uv run lint-imports` 通过。
4. 每个分支合并部署后核 `runtime_git_sha` 与受影响服务的 NRestarts。
