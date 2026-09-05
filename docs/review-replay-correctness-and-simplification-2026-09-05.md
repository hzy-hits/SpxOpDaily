# 回放正确性与测试简化审查（2026-09-05）

基线：`e6ef9dbd9b28974790033c6b9656ca39f3f18cca`，开始时工作树干净。
后续第二轮修复与真实数据验收见 [策略推送数据审计](strategy-push-data-audit-2026-09-05.md)。

本文件保留第一轮阶段记录。范围：执行方案 **S2–S4** 的估值、退出、模型证据修复，**P1-4** 的测试简化；
通知只修现有 Python producer 的证据链接，不修改冻结的 outbox、scheduler、Rust 或部署。

不能从代码推断某个模型生成这些实现时的动机。本次按可复现错误、实际调用关系、
经济不变量判断“多余防护”，不按模型名称或测试是否含有拒绝条件判断。

## Change Brief

- 用户结果：损益方向和止损正确，未完成观测不成为收益标签；测试允许实现简化。
- 现有 owner：`analytics/options/strategy_payoff.py`、`application/order_map/` 的
  path/surface/edge/ranker/delivery/image_delivery，以及 `physical_followthrough.py`、
  `data_platform/research/strategy_*`。存储统一直接使用原 `LatestStateStore`。
- 复用：NumPy、scipy、scikit-learn、pytest、Hypothesis、已有文件输出和通知入口。
- 新依赖：无。无新生产模块、服务、timer、数据库、队列、配置项或 Rust contract。
- 删除：重复估值/风险统计、无调用者的 EV helper、无行为存储子类和重导出、
  源码形状测试、训练器独立 20 分钟退出覆盖与失效的 CLI 参数。
- 持久化：只改变未来生成的研究结果和现有 published 目录下的不可变图片/JSON。
  不修改历史数据，不操作生产进程，不发送消息。
- 最小验收：2.5 点贷记的正确止盈/止损 → 主/备用估值 → policy PnL → 赢家证据；
  再验证完整退出、事后回补、模型时间边界、卡片图片与报价期限。

## 确认并修复

| 问题 | 修复与验收 |
| --- | --- |
| 铁鹰 signed value 被误作正回补成本，亏损方向反转 | 主曲面、sticky 和 physical/clearing 复用同一转换。`buyback_cost=max(close_seed-(model-model0),0)`；2.5→1.25 回补为 +125 美元，2.5→7.5 为 -500 美元，均为费用前。 |
| 负值截断使 3C 止损不可达 | 移除人工 policy coordinate 的零截断；credit 止盈/止损直接比较回补成本。主/备用亏损路径回归测试覆盖真正的 stop_loss。 |
| 三报价行蝶式按三份合约收费 | 路径及研究标签按 `sum(abs(quantity))` 收费；1:-2:1 为四份，双边费用 10.56 美元。 |
| 共享默认退出时刻覆盖候选管理合同 | 删除赢家重复预加载。按候选 policy 加载至 15:45/15:55/次日 12:30；传递 session_date，最后不足一个采样周期时对齐真实退出时刻。 |
| marks_exhausted 当作完成退出 | 不再伪造 exit_at/exit_bid/PnL；未完成标签 PnL 为 null。收益汇总拒绝不完整路径，不用只保留已解决路径的方式制造幸存者偏差。 |
| 报价断层仍生成完整收益 | 研究标签拒绝大于一分钟的组合观测缺口，也不无限沿用缺失腿；joint 按五分钟估值采样检查。只有夜盘起点和 RTH 片段的 clearing 路径返回 quote_gap，不能证明夜间没触发止损。 |
| 训练 20 分钟、实际持有至硬退出 | 训练直接复用当前默认管理合同；删除训练 CLI 的 `--lookforward-minutes`。运行时要求 artifact 管理版本与候选一致，旧工件必须重训。 |
| 模型来自未来 | 同时核对 generated_at、trained_through 与决策/工件日期；训练仅纳入 generated_at 前已完成且可用的标签。 |
| 历史 physical 回补泄漏 | 源分钟和到达时间均不能晚于决策截点。使用 available_at/created_at/observed_at；没有到达时间的旧行不用于严格 as-of。追加未来回补后旧路径保持不变。 |
| 平衡分类输出被称为真实胜率 | 移除 balanced 类权重，使用不惩罚 intercept 的 lbfgs；输出改为 profit_score / early_stop_score，注明 uncalibrated。相同特征、10% 正例测试返回约 10%。 |
| 残差分位修正被称为均值 LCB | 改为 pnl_residual_q10_points，阈值同名更新；保留计算含义，不声称覆盖率或均值置信保证。 |
| 曲面降权重复计入 | ranker 从原 selection_score_base 汇总一次；生成→排名→最终候选回归验证非零 modifier 只计一次。 |
| 人工卡缺少证据/报价边界 | 前向未验证直接进入标题；分开显示观点有效期和本次报价期限。SPX 结构线明确为重新评估条件，退出继续服从管理合同。 |
| 历史卡引用可变图片 | 图片按决策内容 SHA-256 固定路径，旁置完整决策 manifest 和图片哈希；生成成功后才引用。latest/GTH latest 作为实时别名；新图不改变旧卡图。 |
| 样本数和不确定性表述过强 | 路径摘要并列显示路径数与独立 session 数；启发式 sample-shortfall 惩罚明确不是置信区间。未修改已有 session 否决阈值。 |
| 校准摘要凭借记高低推导 promotion | 删除 entry_ask 分组推导可推广的启发式；收益桶按 policy_version 区分，只收完整标签，继续作为描述统计。 |

`PolicyMark.combo_bid` 是现有内部接口：debit 为清算 bid；credit 为 `2C-buyback_cost`，
允许为负，**不是市场报价**。该语义在类型和唯一估值转换处说明，没有另建状态机或兼容层。

观测结果的对应关系：真实退出为 COMPLETE_EXIT；`marks_exhausted` 为 CENSORED；
`quote_gap` 为 QUOTE_GAP；`session_error` 为 SESSION_ERROR。后三者不生成数值收益标签。

## 删除的复杂度

- 九个纯源码形状/别名测试：forecast service integration 三个；gamma-prearm、
  GTH consumer、trade-critical 调用顺序、heavy ES consumer 各一个；projection 架构文件两个。
  另去掉 ES consumer 行为测试尾部的源码顺序断言和存储 roundtrip 中的别名身份断言。
- 保留附近的真实行为测试、新鲜度/因果/定义风险测试、Import Linter、纯计算 I/O 边界、
  数据 roundtrip、通知去重与交付测试。`test_module_size_budget` 没有放宽：路径模块降到
  1000 行以下后删除其债务豁免。
- 删除 `infrastructure/market_data/latest_projection.py` 和仅有 docstring 的
  `LatestMarketProjectionStore` 子类；所有调用方直接使用同一实际实现 `LatestStateStore`。
- 删除 `_BaselinePath` 假对象、只被测试调用的 `apply_policy_ev_score`、
  无调用者的 `_render_strategy_idea_memo`，以及重复风险目标/直方图实现。
- 检查了固定日期 Gamma/Spring 研究脚本的入口。它们是历史复现工具，保留；不能因为
  使用 notebook、固定研究区间或存在保护条件就认定无用。本次未运行这些历史研究包。

## 历史结果和升级边界

| 待重算旧 method | 修复后 method |
| --- | --- |
| physical_path_management_policy.v3 | physical_path_management_policy.v4 |
| physical_path_iron_condor_clear_1230.v1 | physical_path_iron_condor_clear_1230.v2 |
| joint_spot_surface_management_policy.v1 | joint_spot_surface_management_policy.v2 |
| joint_spot_surface_iron_condor_clear_1230.v1 | joint_spot_surface_iron_condor_clear_1230.v2 |
| sticky_iv_same_spot_paths.v1 | sticky_iv_same_spot_paths.v2 |

上述旧分布的人读摘要显示待重算，不能进入 adverse-path 否决。
新的模型 artifact/version 前缀为 `entry_edge.v2:`；旧模型不能通过修改日期冒充新模型。
研究 schema 名称与 Rust wire contract 不变。

未批量修改历史产物或已发送卡片。已有 latest 图片没有当时快照，无法凭现在的图片
恢复历史证据。重算必须使用可追踪到达时间的数据；旧结果保留版本，不能静默改写。
采用 `odte_level_simulation` 等其他真实报价回放的历史研究，必须另按其退出定义审计，
不能据本次错误宣布全部无效，也不能因本次修复宣称它们已验证。

规则、跨策略类型优先序及 forward_unvalidated_user_override 的人工授权没有扩大。
本次不调整 delta、credit 门槛、候选空间或自动下单权限，不开展数据平台/Rust 迁移。

## 验证及局限

- 原仓库最小复现确认了 +125 被算成 -125，-500 被算成 +250/止盈的错误。
- 新增验收覆盖：经济方向与真实止损、四份合约费用、15:55 与非整分钟硬退出、
  不完整观测/缺腿、事后回补、未来/错合同模型、类别基准率、重复降权、不可变图片与卡片期限。
- 第一轮全仓：3283 passed / 2 failed。到达时间夹具已修，重复代码已删除以满足原代码量门。
- 后续相关集合：429 passed；研究集合：163 passed；最终全量结果见下方。
- 未部署、未重启、未检查生产运行版本/行情湖/通知送达，未重算真实历史收益或训练生产模型。
- 本次证明软件行为满足新增断言；未证明独立样本外收益、成交概率或概率校准。
  实际历史样本、摩擦、尾部情景和独立留出 session 仍需单独研究验收。

最终验收：

- `uv run pytest -q --tb=short`：**3286 passed**，100.74 秒；2 条现有第三方弃用提示。
- `uv run ruff check src tests scripts`：通过。
- `uv run lint-imports`：2 contracts kept，0 broken。
- `git diff --check`：通过。
- Rust 未改动，未执行发布/cutover，因此未运行 Rust 发布门。

复杂度记账：生产文件新增 **0** / 删除 **1**；生产 LOC 新增 **326** / 删除 **426**，
净 **-100**。依赖、runtime 配置键、service/timer、数据库/表均 **+0/-0**。
训练 CLI 参数删除 **1**（`--lookforward-minutes`）；测试文件删除 **2**，未新增测试文件。
旧路径删除项见上文。工作树改动尚未 commit/push；无生产部署。
