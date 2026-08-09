# SPX Spark 0DTE 策略信号引擎实施合同 v3

状态：**已批准并开工**（2026-08-09 用户确认「开工吧」）。本文是
`docs/strategy-signal-engine-v2.md` 之上的增量合同，不整体替代 v2。除本文明确修订
的条款外，v2 的全部约束继续有效。

P0 三件套（flood-control 自我阻断、directional butterfly 降权、排序键降级）已于
2026-08-09 合入 master（`bcb9a685`）。后续 P1/P2 工作的设计合同与执行方案见
`docs/strategy-signal-engine-v3-p1p2-design.md` 与
`docs/strategy-signal-engine-v3-p1p2-execution.md`。

实现进度（同一功能分支 `cursor/strategy-engine-v3-design-2238`）：

| 阶段 | 状态 |
|---|---|
| V3-1 多 horizon 打标 + ManagementPolicy + Pass-A 回填 | 已落地 |
| V3-0 人类消息可观测性 | 已落地 |
| V3-2 几何统一 + candidate factory + ranker | 已落地 |
| V3-3a utility 降级为排序权 + 防洪水 | 已落地 |
| V3-3b ManagementPolicy EV 评分与校准报告框架 | 已落地（仅 rank_only，未升门） |

---

## 0. 本文解决什么问题

v2 上线后的生产与回放证据（2026-08-07/08 审计、confirmed-breakout replay、
strategy-decision vertical replay）确认了四类残余问题：

1. **展示层语义错误**：`notifier/prompts.py:60-66` 对普通结构事件固定输出
   "🔴 只观察 / 合约 当前没有可执行合约"，不读取同 cycle 的最终
   `strategy_decision`。NO_TRADE、候选被门吞、投递失败在人类界面上不可区分。
2. **research 概率模型形成 authority inversion**：`strategy_select._utility_gate`
  （`strategy_select.py:348-396`）让合同上声明 advisory / research-unvalidated 的
   P/Q 层拥有对确定性候选的最终否决权（`utility <= 0 or lower_bound <= 0` →
   NO_TRADE），且其 0.75/0.25/0.25/0.5 权重与 entry gate 的 45% debit 上限在数学上
   互相矛盾（45% debit 即 4.5/10 时要求 p > 78.75% 才可能通过）。
3. **概率事件与合约盈亏语义错配**：方向 forecast 阈值取当前 spot、horizon 固定
   300 秒，与候选 breakeven 和真实退出方式（+50% 移动止盈，非持有到结算）无关；
   payoff 被近似为 max_gain/max_loss 二元。
4. **两套几何 + 单候选淘汰器**：`strategy_select._structural_geometry`
  （`strategy_select.py:315-330`）只取单一 call/put wall，与
   `trade_geometry.confirmation_geometry` 的 wall ladder + expected-move fallback
   不一致；蝴蝶枚举后 `max(rows)`（`strategy_select.py:101`）只留一个进 utility，
   Vertical 宽度硬编码 10 点（`_intent_spread` 的 `strike ± 10.0`），第一名失败
   直接 NO_TRADE，没有第二名重试和跨结构竞争。

v3 的目标：在不放宽任何确定性硬门阈值的前提下，
（a）消除展示层歧义；（b）把未校准的 P/Q 从否决权降为排序权；
（c）以用户真实管理规则（+50% profit arm）为中心重建收益语义；
（d）统一几何 owner 并实现多候选竞争；（e）用数据湖回填标签替代前向等待。

---

## 1. 阶段总览与开工序

| 阶段 | 内容 | 依赖 | 交易逻辑变化 |
|---|---|---|---|
| V3-1 | 多 horizon 连续打标（前向）+ ManagementPolicy 标签回填（历史） | 无 | 无 |
| V3-0 | 人类消息可观测性（message_role / decision_id / rejection reason / git sha） | 无 | 无 |
| V3-2 | 几何统一 + candidate factory + 硬门逐候选 + 统一排序 | V3-0 建议先行 | 有（候选生成方式） |
| V3-3a | utility 否决权降为排序权 + 防洪水约束 | V3-2 | 有（NO_TRADE 边界） |
| V3-3b | ManagementPolicy EV 评分 + 首次校准报告 + 升门证据条件 | V3-1 标签、V3-2 候选 | 无（仅排序，升门需再批准） |

开工序：**V3-1 → V3-0 → V3-2 → V3-3a → V3-3b**，中间无数据等待期：
Schwab raw 报价湖自 2026-07-16 起连续覆盖（约 17 个交易会话），V3-3b 的首次
EV 拟合用回填标签当期完成。17 会话是 bootstrap 级样本，因此 v3 全程 EV
**只排序、不否决**；升级为硬门的证据条件见 §7.4，需另行批准。

---

## 2. V3-1：连续打标与历史回填

### 2.1 现状

`strategy_outcomes.observe_due_strategy_outcomes` 每个决策只在 +5 分钟采一次
conservative exit mark（`strategy_outcome_mark.v1`，`label_basis=
decision_quote_shadow_not_fill`）。`outcomes` 表主键已是
`strategy-outcome:{decision_id}:{horizon}m`，天然支持多 horizon，不需要改表。

### 2.2 前向多 horizon marks

- 冻结常量（代码常量，不进 runtime.yaml）：

```python
MARK_HORIZONS_MINUTES = (1, 2, 3, 4, 5, 7, 10, 15, 20)
```

- `read_due_strategy_observations` 增加一次查询多 horizon 的能力（单条 SQL，
  避免每 cycle 9 次查询）；`observe_due_strategy_outcomes` 按到期 horizon 批量
  采样。调用点不变（`market_features/service.py` 现有 cycle）。
- mark schema bump 到 `strategy_outcome_mark.v2`，attributes 增加：
  `spot_spx`、`regime_terminal_state`（采样时点）、`combo_bid`、`combo_ask`。
  不加 IV/greeks（第一版不需要，避免膨胀）。
- 被拒候选照常打标：现有代码已从 `why_not.nearest_candidate` 取腿，V3-2 之后
  改为对 `candidates_considered` 里 top-3 全部打标。
- 写入量评估：每决策 ≤ 9 行 outcomes，RTH 约每分钟 1 个决策 → 每会话
  数千行量级，SQLite 无压力。

### 2.3 ManagementPolicy v1（冻结常量，`management_policy.v1`）

```python
@dataclass(frozen=True)
class ManagementPolicy:
    policy_version: str = "management_policy.v1"
    entry_basis: str = "conservative_combo_ask"
    valuation_basis: str = "conservative_combo_bid"
    profit_arm_return_on_debit: float = 0.50    # bid >= 1.5x debit 视为 arm
    trail_after_arm_fraction: float = 0.75      # arm 后 bid 跌破 peak_bid*0.75 离场
    trail_floor_is_entry_debit: bool = True     # trail 线不低于成本价
    premium_stop_fraction: float = 0.50         # bid <= 0.5x debit 止损
    time_stop_minutes: int = 20
    hard_exit_et: str = "15:45"
    fees_per_leg_per_side: float = 1.32         # 与既有 replay 口径一致
```

出厂值是待校准的初始冻结值；回填校准报告可提议 `management_policy.v2`，
改参数 = 改代码 + 版本递增 + 回放对照（沿用 S-track 阈值规则）。

### 2.4 标签模拟器（放入现有 owner `analytics/options/strategy_payoff.py`，不新增文件）

```python
def simulate_management_policy(
    marks: Sequence[Mark],          # 按时间升序的 (at, combo_bid) 序列
    *, entry_ask: float, policy: ManagementPolicy,
) -> PolicyLabel
```

输出：`tp_armed`、`tp_before_stop`、`time_to_arm_seconds`、`mfe_points`、
`mae_points`、`policy_pnl_points`（含费用）、`exit_reason ∈ {trail, premium_stop,
time_stop, hard_close, marks_exhausted}`、`quote_gap_seconds_max`。
纯函数、无 I/O；不变量用 Hypothesis 测试（见 §9）。

### 2.5 历史回填（新增研究脚本 `data_platform/research/strategy_policy_backfill.py`）

两个 pass，均为一次性离线脚本，不进 systemd：

- **Pass A（V3-1 即可跑）**：对 `spx.sqlite` 已记录的每个 decision（含
  nearest_candidate），从 `raw/provider=schwab`（2026-07-16 起）抽取决策时刻后
  20 分钟的逐腿 NBBO，重建 conservative combo bid 序列，跑
  `simulate_management_policy` 产出标签。
- **Pass B（V3-2 之后）**：对 v2 §19.1 的固定决策时点 + confirmed trigger 时点，
  用 candidate factory 的同一枚举代码重建"当时应有的候选表"，全部打标。
  这是 EV 拟合的主数据集。
- 输出写入 `features/strategy_policy_labels/`（沿用现有 features parquet lake
  模式，不新建存储系统；因果字段 `decision_at`/`available_at` 必带）。
- 已知偏差（写进产出报告）：Pass B 候选是按新逻辑重建的假想候选，与当时
  生产真实候选不同集；GTH 时段 IBKR 覆盖稀疏，标签以 RTH 为主。

---

## 3. V3-0：人类消息可观测性

### 3.1 message_role

每条人类可见消息必须带一个角色，判定规则固定：

| message_role | 判定 |
|---|---|
| MANUAL_CANDIDATE | 同 cycle `strategy_decision.action_authority == "manual"` |
| NO_TRADE | 同 cycle `strategy_decision.decision_type == "NO_TRADE"` |
| STRUCTURE_EVENT | 普通结构/流量事件，且同 cycle 策略决策不可用或与事件无关 |
| SYSTEM / POSITION | 现有 system/position 分支不变 |
| DELIVERY_DEGRADED | 最近 30 分钟 outbox 存在 failed/uncertain 投递（附注形式） |

### 3.2 `notifier/prompts.py` 改造

- `format_alert_message` 增加读取 `payload["strategy_decision"]`：
  - 仅当同 cycle `decision_type == "NO_TRADE"` 时才允许输出
    "合约 当前没有可执行合约"，并且必须同时输出
    `原因 {why_not.reasons[0]}`；
  - 若同 cycle 存在 MANUAL_CANDIDATE，事件消息改写为
    "合约 本周期存在人工候选（{decision_id 短码}），以候选卡为准"；
  - `strategy_decision` 缺失时写 "策略决策 本周期不可用"，不得伪装成 NO_TRADE。
- 每条消息尾部固定审计行（现有 `数据 as_of=...` 行扩展）：

```text
决策 id={decision_id[:12]} 角色={message_role} 原因={top_rejection_reason}
版本 policy={policy_version} git={runtime_git_sha}
```

### 3.3 runtime_git_sha

在 `settings/loader.py` 增加 `runtime_git_sha()`：进程启动时执行一次
`git -C <repo_root> rev-parse --short HEAD`（失败则读环境变量
`SPX_SPARK_GIT_SHA`，再失败为 `unknown`），结果缓存。注入
`strategy_decision`（§5.4）与消息审计行。这直接治理"master 已修但进程跑旧码"
的部署盲区。

### 3.4 DELIVERY_DEGRADED

卡片无法自知投递失败，因此采用事后附注：通知组装时查询
`notification_events` 最近 30 分钟 failed/uncertain 计数，非零则在审计行追加
`投递 degraded(n)`。不新增服务，不改 outbox 语义。

---

## 4. V3-2：单一几何 + candidate factory + 统一排序

### 4.1 几何统一（删除 `_structural_geometry`）

- `trade_geometry.confirmation_geometry` 是唯一 target/stop 来源（wall ladder +
  `expected_move_confirmation_floor_fallback` 语义保留）。
- market_features 阶段已计算的 confirmation geometry 通过 fact pack 透传给
  selector；`strategy_select._structural_geometry`（315-330 行）**删除**。
- `evaluate_trade_intent` 降级为 trigger/quote evidence producer：保留 breakout
  filter、exact quote、repricing，**移除**其独立的最终 target-room/RR 判断输出
  对人类语义的影响（保留字段仅作 evidence）。

### 4.2 candidate factory（新增 `application/order_map/candidate_factory.py`）

```python
def enumerate_candidates(
    payload, facts, regime, latest, *, now, policy,
) -> list[dict]   # CandidateRow
```

枚举表（仍限定 v2 §1.4 的五类）：

| 结构 | 枚举维度 | 报价来源 |
|---|---|---|
| Call/Put Debit Vertical | long ∈ {trigger 邻近 strike, spot 邻近 strike} × width ∈ {5,10,15,20} | RTH：Schwab 全链 `latest.best_quote`；GTH：维持现有单 exact-leg 路径，不枚举 |
| Call/Put Butterfly | center ∈ pin top_centers ∪ {confirmation target, q_mode} × width ∈ {5,10,15,20} × {C,P} | 同上 |

语义变化：

1. **regime 降为先验**：PIN_STABLE 不再排他生成蝴蝶，trend/confirmed-trigger
   状态同时生成 directional butterfly（center = confirmation target）；
   PIN_STABLE 下若存在 confirmed trigger 也生成 vertical。regime 只贡献排序
   先验分量，不再决定"生成什么"。
2. **candidate_id**：`sha256(session_date, strategy_type, expiry, strikes,
   right)[:16]`，稳定可引用，供打标（§2.2）与后续 LLM 层使用。
3. 运营约束：多宽度枚举仅在 RTH 用 Schwab 链数据，**不新增 IBKR ticker line
   占用**（84 条 option lane 预算不变）。

### 4.3 统一硬门与排序（新增 `application/order_map/strategy_ranker.py`）

```python
def rank_candidates(rows, facts, regime, *, policy) -> RankResult
# RankResult: passed(排序后), near_misses(top3, 带具体失败门与差值), gate_audit
```

- 确定性硬门逐候选执行，**任何阈值不放宽**：conservative BBO status/freshness/
  skew、`vertical_entry_quality`（debit fraction、target room、stop ATR、
  late chase）、蝴蝶三腿完整性、TTL、event gate、session cutoff。
- 通过硬门的候选全部评分排序，取第一名；第一、二名均保留在 payload。
- 全部失败 → NO_TRADE，`why_not.nearest_candidate` 扩为 **top-3 near-miss**，
  每项含 `{candidate_id, strategy_type, strikes, failed_gates: [{gate, actual,
  threshold}]}`。
- 评分函数：V3-2 阶段沿用现有 utility 公式（仅排序用途，见 §6）；V3-3b 换
  ManagementPolicy EV。
- `_select_butterfly` 的 `max(rows)` 单选与 `_intent_spread` 的 ±10 硬编码在
  同一 PR 内删除（枚举职责移交 factory）。

### 4.4 schema：`strategy_decision.v2`

新增字段（不进 `contracts/golden/`、不进任何 Rust 投影，纯 Python 演进）：

```text
runtime_git_sha
geometry_source            # "confirmation_geometry"
candidate.candidate_id
candidates_considered      # 最多 5 项：{candidate_id, strategy_type, strikes,
                           #   score, gate_failures}
why_not.nearest_candidates # top-3（替代单个 nearest_candidate，保留旧键一个
                           #   release 供 desk projection 迁移，随后删除）
```

`policy_version` 递增：`strategy_policy.bootstrap.v1 → v2`。

---

## 5. V3-2 回放验收门

沿用 v2 §19 框架，追加：

1. 决策时点 = v2 §19.1 固定时点 + confirmed trigger 时点，覆盖 7/16–8/8 全部
   可用会话（≥15 个）。
2. 冻结案例 8/5、8/6 按 v2 §20 继续通过。
3. 新增对照案例 **8/7、8/8**：
   - 8/7 14:0x 的 confirmed breakout（确认时已走完约 78% 目标、距 Call Wall
     <1 点）必须仍为 NO_TRADE，且 near-miss 明确给出
     `direction_valid_but_entry_too_late` 与进度数值 —— anti-chase 语义不回退；
   - 8/7 曾因 selector 断链产生的 27 个 `vertical_exact_spread_unavailable`
     在新枚举下不得复现。
4. 逐 cycle diff 报告：老 vs 新 `decision_type`、候选数、第一名。要求
  （a）老逻辑的每个 MANUAL_CANDIDATE 在新逻辑下仍在或有书面解释；
  （b）新增候选逐个列出硬门通过证据；
  （c）`available_at <= decision_at` 全过。

---

## 6. V3-3a：utility 降级为排序权 + 防洪水

- `_utility_gate` 更名 `_score_candidate`，**删除否决路径**（394-395 行的
  `return None`）。概率证据缺失/事件错配/utility 非正不再产生 NO_TRADE，
  降级为候选的 `edge` 块：

```python
"edge": {
    "edge_status": "research_unvalidated",
    "utility": ...,                # 仅排序
    "required_p_breakeven": ...,   # (1+0.75)*debit/width，显式暴露
    "model_p": ...,
    "advisories": [...],           # 原否决理由降级为标注
}
```

- 是否成 MANUAL_CANDIDATE 完全由 §4.3 确定性硬门决定。
- **防洪水冻结常量**（进 `StrategyPolicy`，随 policy_version v2）：

```python
candidate_cooldown_seconds: float = 300.0   # 同 setup_kind+direction+trigger_level
max_cards_per_direction_per_session: int = 6
```

  超限候选仍写入 payload 与打标管道，但不发通知卡。
- 人工卡必须显示 `required_p_breakeven` 与 `model_p`，让使用者看见
  "这张卡需要 xx% 概率才保本"。

---

## 7. V3-3b：ManagementPolicy EV 与升门条件

### 7.1 评分

```text
Score = EV_policy / MaxLoss
      - 0.50 * ES10 / MaxLoss
      - 0.25 * LiquidityPenalty
      - 0.25 * ModelUncertainty
```

`EV_policy`、`ES10` 来自 §2 标签数据集上按（regime、setup_kind、结构、进度带）
分桶的经验分布；λ 为冻结常量（v1 出厂值如上，改动走版本递增）。

### 7.2 数据要求

Pass B 标签 ≥ 15 个会话；每个使用中的分桶 `n_effective ≥ 8`，不足的桶回退到
上级桶并在 `edge.advisories` 标注 `bucket_fallback`。

### 7.3 权限

V3-3b 全程 **只排序**。NO_TRADE 只能由确定性硬门产生。

### 7.4 升门证据条件（达成后需用户再批准 + policy_version 递增）

1. walk-forward（按周分割，无重叠）下，Score 对 `policy_pnl > 0` 的判别
   AUC ≥ 0.60，且最高分位与最低分位的平均 policy_pnl 差为正；
2. 校准误差（分桶预测 EV vs 实际均值）≤ 0.15 × MaxLoss；
3. 覆盖 ≥ 25 个会话（回填 + 前向合计）；
4. 冻结案例与 8/7、8/8 对照在升门后复跑仍通过。

---

## 8. 删除清单（与实现同 PR）

| 项 | 位置 | 处置 |
|---|---|---|
| `_structural_geometry` | strategy_select.py:315-330 | 删除（V3-2） |
| 蝴蝶 `max(rows)` 单选 | strategy_select.py:101 | 删除，移交 ranker（V3-2） |
| `_intent_spread` ±10 硬编码枚举 | strategy_select.py:248-262 | 降级为 evidence，宽度枚举移交 factory（V3-2） |
| `_utility_gate` 否决路径 | strategy_select.py:394-395 | 删除（V3-3a） |
| 固定文案"合约 当前没有可执行合约" | notifier/prompts.py:66 | 改为条件输出（V3-0） |
| `why_not.nearest_candidate` 单数键 | strategy_select.py | 双写一个 release 后删除 |

---

## 9. 测试计划（与改动成比例）

- **纯函数单测**：`simulate_management_policy` 的 Hypothesis 不变量
  （`-debit - fees <= policy_pnl <= width - debit - fees`；`mae <= 0 <= mfe`；
  marks 单调时间；arm 后 exit 不低于 trail 线）；candidate factory 枚举确定性
  （同输入同 candidate_id 集）；ranker 硬门顺序与 near-miss 差值。
- **消息层单测**：message_role 判定矩阵（decision 缺失 / NO_TRADE /
  MANUAL_CANDIDATE / degraded 四象限）。
- **冻结回放**：8/5、8/6、8/7、8/8 四个会话作为语义回归；不因此新增全量
  pytest 到日常门槛，全量验证保留为各阶段合并 gate。

---

## 10. 复杂度预算（Complexity Budget）

| 项 | 数量 |
|---|---|
| 新增生产文件 | 2（candidate_factory.py、strategy_ranker.py） |
| 新增研究脚本 | 1（strategy_policy_backfill.py，一次性离线） |
| 删除/降级 legacy 路径 | 6（§8 清单） |
| 净生产 LOC 预估 | ≤ +900（v2 §21.3 预算之外，本合同重设上限：生产 ≤1,000 / 测试 ≤600） |
| 新依赖 | 0 |
| 新 config key | 0（全部冻结代码常量 + 版本号） |
| 新 service/timer | 0 |
| 新数据库/表 | 0（outcomes 复用；新增 1 个 features parquet 数据集 `strategy_policy_labels/`，沿用现有 lake 模式） |
| Rust / contracts/golden 变化 | 0 |

---

## 11. 需要用户明确批准的事项

1. 新增 2 个生产文件 + 1 个研究脚本（v2 §18.1 五文件上限由本合同接管并重设）。
2. schema bump：`strategy_decision.v2`、`strategy_outcome_mark.v2`。
3. `strategy_policy.bootstrap.v2`（含防洪水常量：冷却 300s、每方向每会话 6 张卡）。
4. `management_policy.v1` 出厂值（§2.3，尤其 +50% arm、trail 0.75、premium stop 0.5、20 分钟 time stop、15:45 ET 硬退出）。
5. 新增 `features/strategy_policy_labels/` parquet 数据集。
6. V3-3b 升门证据条件（§7.4）作为未来再批准的固定门槛。

批准方式：在任务对话中明确回复；批准后按 §1 顺序开工，每阶段独立 commit 与
回放验收，验收证据落 `docs/research/`。
