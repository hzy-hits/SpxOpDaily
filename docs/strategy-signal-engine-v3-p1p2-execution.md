# SPX Spark 0DTE 策略信号引擎 v3 P1/P2 执行方案

状态：**已批准，按任务卡顺序开工**。设计合同与决策记录见
`docs/strategy-signal-engine-v3-p1p2-design.md`（本文引用其 D1–D6 与 §2 语义）。

## 1. 分支与开工序

| 分支 | 任务卡 | 依赖 |
|---|---|---|
| `cursor/strategy-p1-persistence-labels-2238` | P1-A1 shadow 持久化、P1-A2 结构失效标、P1-A3 删失标、P1-A4 机会去重 | 无 |
| `cursor/strategy-p1-ev-rank-only-2238` | P1-B1 EV 工件产出、P1-B2 ranker 附着与卡片展示 | P1-A 全部（标签质量决定 EV 表可信度） |
| `cursor/strategy-p2-idea-memo-2238` | P2-C1 memo 生成与校验、P2-C2 卡片渲染 | 无（可与 P1-B 并行） |

每张任务卡 = 一个独立 commit。每个分支合并 master 前跑全量验证；
部署逐分支进行，不合批。

## 2. P1-A 任务卡

### P1-A1 shadow 候选持久化（设计 §2.1，决策 D1）

改动文件与内容：

1. `src/spx_spark/application/order_map/strategy_select.py`
   - `build_strategy_decision` 的选中分支把 `rank.passed[1:3]` 全量候选放进
     decision payload 新字段 `shadow_candidates`（list，最多 2 项）；
     腿/报价不完整的候选进 `shadow_candidates_skipped`（只存
     `candidate_id` + 原因）。
   - NO_TRADE 分支两字段恒为空列表。
2. `src/spx_spark/infrastructure/operational_db.py`
   - 新函数 `persist_strategy_shadow_candidates(decision, *, database_path=None)
     -> tuple[str, ...]`：对每个 shadow 候选构造合成行
     `decision_id = f"{parent_id}:cand{rank}"`、`status="shadow_candidate"`、
     `attributes_json` 存候选级合成 payload（父 id、rank、候选全量、
     policy_version、decision_at/available_at）；腿复用 `_leg_rows`；
     `_leg_rows` 抛错的候选跳过并返回时排除。与父决策同一个
     `engine.begin()` 事务外独立提交（父决策失败不阻塞、shadow 失败不回滚父行）。
   - `read_strategy_decisions` 加 `decisions.c.status.in_(("selected", "no_trade"))`
     过滤，防 replay 污染。
3. `src/spx_spark/application/market_features/service.py`
   - 现有 `persist_strategy_decision` 调用点之后追加
     `persist_strategy_shadow_candidates` 调用，结果并入
     `strategy_decision_persistence` 观测字段（`shadow_persisted_ids`）。

不改动（明确）：`recent_selected_strategy_cards`（status 过滤天然排除 shadow）、
`read_due_strategy_observations`（不看 status，shadow 自动进标记管道）、
`enqueue_strategy_decision`（shadow 不投递）。

测试（`tests/test_order_map.py` 或就近既有文件）：

- shadow 行落盘：selected 决策 + 2 个 rank 2/3 候选 → `decisions` 出现
  `:cand1`/`:cand2` 行、腿数正确、`status="shadow_candidate"`。
- 标记管道自动覆盖：`read_due_strategy_observations` 返回 shadow 行的到期观察。
- replay 防污染：`read_strategy_decisions` 不返回 shadow 行。
- flood control 回归：shadow 行不计入配额（复用现有
  `test_strategy_flood_control_counts_outbox_accepted_cards_not_own_decision`
  场景加一行断言）。

### P1-A2 结构失效标签（设计 §2.2，决策 D2）

改动文件与内容：

1. `src/spx_spark/application/order_map/strategy_outcomes.py`
   - 新私有 helper `_invalidation_breach(decision, candidate, *, data_root,
     decision_at, target_at) -> dict`：读 `data_root/features` SPX 分钟样本，
     按设计 §2.2 规则返回 `{"invalidation_breached": bool | None,
     "breach_at": str | None, "breach_scan_gap": bool}`。
   - `_observe` 把上述字段并入 outcome `attributes`，并置
     `label_kind = "structural_exit" if breached else "horizon_mark"`。
   - `observe_due_strategy_outcomes` 增加 `data_root` 参数（调用方
     `market_features/service.py` 已持有 `storage.data_root`）。
2. `src/spx_spark/application/market_features/service.py`
   - `observe_due_strategy_outcomes` 调用点传入 `data_root`。

前向不定价 breach 时点退出（D2）；离线定价归 P1-B1 的回填侧。

测试：

- 合成分钟序列触及失效价 → `invalidation_breached=true` 且 `breach_at` 为首个
  触及分钟；未触及 → `false`；窗口内缺口超过 2 连续分钟 → `null` +
  `breach_scan_gap=true`。
- UP/DOWN 两方向各一例（Hypothesis 或参数化均可）。

### P1-A3 删失标签（设计 §2.3，决策 D3）

改动文件与内容：

1. `src/spx_spark/infrastructure/operational_db.py`
   - `read_due_strategy_observations`：`lag > maximum_lag_seconds` 的观察点
     不再跳过，改为携带 `"censor_hint": "service_gap"` 返回（每对
     decision/horizon 仍只返回一次，由 outcomes 表幂等键保证）。
2. `src/spx_spark/application/order_map/strategy_outcomes.py`
   - `_observe` 状态收敛：可完整定价 → `observed`；否则 → `censored` +
     `censor_kind`（优先级：`service_gap` > `session_end_before_horizon` >
     `breach_quote_unavailable` > `quote_gap`）。
   - `session_end_before_horizon` 判定：`target_at` 晚于 `session_date` 的
     16:00 America/New_York。
   - `censored` 行的 `spx_return_bps` / `option_return_bps` 强制 `NULL`。
3. `src/spx_spark/data_platform/research/strategy_policy_backfill.py` 与
   `strategy_v3_freeze_acceptance.py`
   - 读取侧把历史 `exit_quote_unavailable` 映射为 `censored(quote_gap)`；
     统计输出增加删失分布段（每 censor_kind 计数）。

测试：

- 四种 censor_kind 各一例；`service_gap` 行返回值为 NULL 的不变式；
- 幂等：同一 (decision, horizon) 已有删失行后不再重复写。

### P1-A4 机会级去重（设计 §2.4，决策 D4）

改动文件与内容：

1. `src/spx_spark/infrastructure/operational_db.py`
   - `persist_strategy_decision` 事务内：候选存在时先
     `sqlite_insert(events).on_conflict_do_nothing()` 写机会行
     （`event_key = f"strategy-opportunity:{session_date}:{opportunity_id}"`、
     `event_type="strategy_opportunity"`、`source_at/available_at` 取决策时间），
     再把 event_key 写入 decision 行；NO_TRADE 决策 event_key 保持 NULL。
   - `persist_strategy_shadow_candidates` 的 shadow 行挂同一 event_key。
2. `src/spx_spark/data_platform/research/strategy_policy_backfill.py` 与
   `strategy_v3_freeze_acceptance.py`
   - 聚合按 event_key 分组，每机会只计首个被 outbox 接受的决策；后续决策标
     `duplicate_of=<首个 decision_id>` 保留在明细里。

测试：

- 同一 opportunity 两次持久化 → 一行 `events`、两行 `decisions` 同 event_key；
- 外键完整性：event 行先于 decision 行存在（事务内顺序断言）；
- 聚合去重：三个同机会决策 → 统计计 1，明细 3 行含 `duplicate_of`。

### P1-A 分支级验收

1. 冻结回放 2026-08-05 / 2026-08-06 决策输出逐字段与 master 一致。
2. `uv run pytest -q tests/test_order_map.py tests/test_strategy_payoff.py` 及
   新增测试全绿，然后全量 pytest + ruff + lint-imports。
3. 合并部署后，在一个真实 RTH 会话内 SQL 抽查：

```sql
SELECT status, COUNT(*) FROM decisions
 WHERE strategy_name='strategy_signal_engine_v2' AND session_date=:today
 GROUP BY status;
SELECT json_extract(attributes_json,'$.censor_kind'), COUNT(*) FROM outcomes
 WHERE created_at >= :deploy_at GROUP BY 1;
SELECT event_key, COUNT(*) FROM decisions
 WHERE event_key LIKE 'strategy-opportunity:%' GROUP BY event_key;
```

## 3. P1-B 任务卡

### P1-B1 EV 工件产出（设计 §2.5，决策 D5）

改动文件与内容：

1. `src/spx_spark/data_platform/research/strategy_policy_backfill.py`
   - 新增 `--emit-ev-table <path>` 参数：按
     `setup_kind × direction × regime_terminal_state` 分桶聚合 ManagementPolicy
     标签（含 P1-A2/A3 之后的 structural/censored 语义：删失行不计入 EV 均值，
     单独计数入桶元数据），写出
     `policy_ev_table.v1.json`（schema 见设计 §2.5）。
   - 生产落点：`data_root/research/policy_ev_table.v1.json`（运行时只读）。
2. 不新增脚本文件；复用现有回填 CLI。

测试：

- 分桶正确性与 `n >= 20` 门槛（不足样本桶输出 null + reason）；
- 删失行不进 EV 均值但进桶计数。

### P1-B2 ranker 附着与卡片展示

改动文件与内容：

1. `src/spx_spark/application/order_map/strategy_ranker.py`
   - 模块级 `lru_cache` + mtime 失效的加载器读取
     `data_root/research/policy_ev_table.v1.json`；`_score_candidate` 之后把
     `policy_ev` / `policy_ev_n` / `policy_ev_version` / `policy_ev_reason`
     并入候选 `edge`。**`rank_candidates` 的排序键不改**。
2. `src/spx_spark/application/order_map/delivery.py`
   - `_render_strategy_candidate` 的 Edge 行追加
     `· policyEV={edge.policy_ev}({edge.policy_ev_n})`；无值时显示
     `policyEV=n/a`。

测试：

- 附着后排序不变（同一输入，有/无 EV 表输出的 passed 顺序一致——这是
  rank-only 边界的回归锚）；
- 工件缺失 → `policy_ev_reason="table_unavailable"` 且流程无异常。

### P1-B 分支级验收

冻结回放对照（EV 表就位与否两种情形），全量验证，合并部署后核卡片文本
Edge 行与 `runtime_git_sha`。

## 4. P2-C 任务卡

### P2-C1 memo 生成与校验（设计 §2.6，决策 D6）

改动文件与内容：

1. `src/spx_spark/notifier/llm_writer.py`
   - 新函数 `call_strategy_idea_memo(decision, settings=None)
     -> tuple[dict | None, str | None]`，模式与 `call_hypothesis_critic` 一致：
     JSON mode、系统提示禁止创造价格/概率/合约/执行指令、6 秒硬超时。
   - 校验函数 `idea_memo_output_valid(memo, decision) -> bool` 实现设计 §2.6
     的四条规则（watch_levels 数值白名单、禁词表、无凭空数字、长度上限）。

测试：

- 合法 memo 通过；watch_levels 含 payload 外数值 → 拒绝；含禁词 → 拒绝；
  超长 → 拒绝；LLM 返回非 JSON → `(None, reason)`。

### P2-C2 卡片渲染

改动文件与内容：

1. `src/spx_spark/application/order_map/delivery.py`
   - `enqueue_strategy_decision` 在 dedup 与 flood 检查通过、渲染正文之后调用
     `call_strategy_idea_memo`；校验通过则在正文末尾追加
     `Idea Memo (research)` 段（thesis / falsification / watch_levels / risks
     各一行），任何失败静默省略并在返回值加 `idea_memo="omitted:<reason>"`
     观测字段。

测试：

- memo 成功 → 卡片含该段；失败/超时 → 卡片主体逐字节不变、返回值含省略原因；
- memo 不改变 event_id、flood 判定与 outbox 行为（投递回归）。

### P2-C 分支级验收

全量验证 + 冻结回放（memo 关闭路径必须与 master 输出一致）；部署后用一次
persist→enqueue 冒烟确认卡片段落与省略路径。

## 5. 复杂度预算（Complexity Budget）

| 维度 | P1-A | P1-B | P2-C |
|---|---|---|---|
| 生产文件新增/删除 | 0 / 0 | 0 / 0 | 0 / 0 |
| 净生产 LOC（估） | +180 | +90 | +130 |
| 依赖增/删 | 0 / 0 | 0 / 0 | 0 / 0 |
| 配置键增/删 | 0 / 0 | 0 / 0（工件不是配置） | 0 / 0 |
| 服务/timer 增/删 | 0 / 0 | 0 / 0 | 0 / 0 |
| 数据库/表增/删 | 0 / 0 | 0 / 0 | 0 / 0 |
| 移除的遗留路径 | `read_due` 对超期观察点的静默丢弃 | 无 | 无 |

任何一项超出本表（尤其出现新表、新文件、新服务的需要）先停工问用户，
不得先做后报。

## 6. 部署与回滚

- 逐分支合并 master 后用 `bash scripts/install-spx-spark-services.sh --now`
  部署；受影响服务为 `spx-core`（market-features tick 内的持久化/标记/投递
  路径）与 `spx-worker`。
- 部署后核验清单：`runtime_git_sha`、`systemctl --user show spx-core spx-worker
  -p NRestarts`、`journalctl` 无新异常、P1-A §2 的 SQL 抽查。
- 回滚 = `git revert` 对应 merge 后重新部署；shadow 行/删失行是纯附加数据，
  回滚不需要清理数据库。
