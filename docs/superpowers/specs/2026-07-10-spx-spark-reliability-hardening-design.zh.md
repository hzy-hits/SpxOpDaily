# SPX Spark 可靠性加固设计

状态：已于 2026-07-10 在对话中批准。本文档在开工前固化已批准的架构。

## 1. 范围与证据

本批次针对一条贯通的可靠性问题，覆盖五条路径：

1. SPXW 持仓浮盈亏与持仓事件投递。
2. 报价新鲜度，尤其是 delayed / delayed-frozen 行情。
3. IBKR 慢轮询与 SPXW ATM 订阅重规划。
4. 将 Hyperliquid SP500 作为非常规时段研究参考，但不允许它成为可执行定价锚。
5. 交易日换日、盘后完备性、测试、Ruff 与 CI。

当前生产基线为 commit `e927694`。已观察到的相关故障包括：

- 两张多头合约 `avgCost=3200`、mark `25` 被报成 `+1800`，正确应为 `-1400`：平均成本只减了一次，没有按合约张数处理。
- 持仓状态在告警评估时就已落盘，早于任何通知成功。因此通知失败或关闭时，开仓、平仓、调仓与浮盈亏事件会被“吃掉”。
- delayed 报价永远不会老化为 stale，仍可进入多条时效敏感路径。
- 19 个慢标签按每批 6 个、每次 hold 10 秒，会把唯一的 stream 循环每轮阻塞约 40 秒。
- 线上日志反复出现围绕 7480、7510、7545 的 ATM 规划。直接原因之一是用错会话的 `ES.close - SPX.close`：`7588.75 - 7482.71 = 106.04`，错误地把约 7587.5 的实时 ES 映射成约 7481.5 的 SPX 参考价。
- 重规划会先取消整套期权订阅再重建，扩大覆盖空洞并加重 IBKR pacing 压力。
- 2026-07-08 盘后报告被标为 complete，但其最终近月 IV/gamma 覆盖率仅约 28%。
- 当前研究到期日在 16:15 ET 换日，只跳过周末，与盘后就绪条件及其他“仅工作日”日历逻辑不一致。
- 当前质量基线：360 个测试中有 1 个失败，另有 1 个 Ruff F401。

## 2. 目标

- 对任意带符号数量与有效 SPXW 乘数，算出正确浮盈亏。
- 持仓事件采用至少一次（at-least-once）投递语义，带确定性事件 ID 与按事件确认。
- 近期 delayed 数据仍可作为显式降级的研究上下文；但 delayed / stale / 未锚定数据不得驱动时效告警或可执行定价。
- 防止数据源来回跳变与不必要的订阅重建，同时不掩盖真实持续行情。
- 热路径 SPX/SPXW flush 节奏与慢轮询解耦。
- 保留 Hyperliquid 在非常规时段的研究价值。
- 研究用 0DTE 在 `America/New_York` 17:00:00 切到下一个有效美股交易日。
- 让 `complete` 真正表示盘后数据集在覆盖率、时效、广度与质量上达标。
- 最终达到：pytest 全绿、Ruff 通过、workflow CI 通过、回放测试通过，以及生产只读 shadow soak。

## 3. 非目标与安全边界

- 不下单、不撤单、不执行、不做任何自动账户操作。
- 不做完整 asyncio / event-bus 重写。
- 不引入 SQLite 或外部队列；仍保持单机、文件落盘。
- 不扩大 IBKR 账户订阅范围，仅保留现有显式开启的只读持仓 watcher。
- 不允许 Hyperliquid 单独作为可执行限价、触达概率或模型重定价的唯一来源。
- 日志、fixture、报告、提交中不得出现密钥或账户标识。

持仓监控仍为 opt-in，使用独立 client ID，并以 `readonly=True` + `StartupFetch.POSITIONS` 连接。文档会明确区分：该经授权的 watcher 与纯行情采集器不同。

## 4. 架构

选定方案是一组小型领域控制器，由现有同步 service loop 协调：

- `PositionEventStore`：持久化观察状态与待投递持仓事件。
- `QuoteUseDecision`：研究 / 告警 / 定价用途的统一判定。
- `AtmReferenceController`：来源溯源、有效 ES basis、稳定 ATM。
- `OptionReplanController`：滞后带、确认次数、冷却、计划 diff。
- `SlowPollScheduler`：协作式 `idle/holding` 状态机。
- `SpotResolution`：研究参考价与定价参考价分离。
- `MarketCalendar`：交易时段、假期、提前收盘、研究到期日。
- `ReviewCompletenessPolicy`：结构化盘后完备性检查。

这些组件是纯逻辑或接近纯逻辑。IBKR 连接与订阅仍在 `StreamCollector`；通知投递仍在 notifier 包；报告渲染仍在 `post_close_review.py`。

## 5. 持仓快照与浮盈亏

### 5.1 快照版本 2

watcher 写入带版本的快照，包含：

- `schema_version=2` 与确定性 `snapshot_id`。
- `fetched_at` 与 `fetch_complete`。
- 托管账户数、券商原始持仓数、过滤后的 SPXW 持仓数。
- 每条腿：带符号数量、券商平均成本、解析后的乘数、mark、mark 来源、mark 质量、源报价时间、最近观察更新时间、mark 年龄。

`fetch_complete=true` 表示只读连接完成，且券商持仓列表在无异常情况下取到。一次成功的“完整空仓快照”不同于失败或不完整的空快照。

含账户状态的文件以原子方式写入，权限 `0600`。写入器对临时文件与最终路径都设置该权限。

### 5.2 浮盈亏公式

按 IBKR 期权持仓约定，`avgCost` 是每张合约成本（已含乘数），`qty` 是独立的带符号数量：

```text
unit_mark_value = mark * multiplier
unrealized_pnl = qty * (unit_mark_value - avg_cost)
cost_basis = abs(qty * avg_cost)
unrealized_pnl_pct = unrealized_pnl / cost_basis * 100
```

账面成本为所有已接受 mark 的腿的 `sum(abs(qty * avg_cost))`。乘数来自合约；无效或缺失时，仅对已验证的 SPXW 期权回退为 100。

结构性持仓事件不要求市价 mark。浮盈亏事件要求新鲜、可执行的期权 mark。delayed / stale / missing / unknown 的 mark 可作为降级参考展示，但不得发出 `quality=live` 的浮盈亏告警。

账面浮盈亏事件要求每条非零 SPXW 腿都有新鲜、可执行的 mark。快照记录 `priced_leg_count`、`total_leg_count`、`book_pnl_complete`。覆盖不全时，分子分母只对同一批已定价腿计算，并标为 partial 供展示；不创建账面浮盈亏事件。

## 6. 持久化持仓事件流

### 6.1 状态模型

`PositionEventStore` 是带锁、原子替换、权限 `0600` 的 JSON 文件：

```text
schema_version
observed_snapshot_id
observed_at
observed_positions
pending_events[]
last_acknowledged_book_pnl
updated_at
```

每条 pending 事件含确定性 `event_id`、快照 ID、类型、标的、旧/新数量或浮盈亏桶、创建时间。事件 ID 在重试间保持稳定。

结构性事件保持有序，永不合并。若账面浮盈亏事件仍 pending，更新的快照会把它合并到相对 `last_acknowledged_book_pnl` 的最新合格桶与严重级别。这样可避免每个 poll 都积压一条未发送浮盈亏，同时保留最新可行动的盈亏信息。

### 6.2 事务顺序

1. 加载事件库与 notifier 已发送状态。
2. 渲染/发送前，先对账：凡 notifier 已持久化为已确认的 pending 事件 ID，先清掉。
3. 加载并校验当前快照。
4. 不完整、过期、未来时间或非单调的新快照，不得用于派生新事件。
5. 将有效新快照与 `observed_positions` 比较。
6. 在同一次文件替换中原子追加新事件，并推进已观察快照。
7. 将所有剩余 pending 事件渲染为携带 `event_id` 的告警。
8. 发送通知。
9. 把成功事件 ID 写入 notifier 已发送状态，并在 `NotificationResult.acknowledged_event_ids` 中返回。
10. 将这些 ID 对账进 `PositionEventStore`，只移除匹配的 pending 事件。仅当对应浮盈亏事件被确认时，才更新 `last_acknowledged_book_pnl`。

至少一个真实人类通道（当前为飞书或 Bark）成功后，持仓事件才可确认。`--no-notify`、无启用 sink、策略过滤、全部 sink 失败，都会让事件继续 pending。

notifier 已发送状态也会保存已确认事件 ID。若进程在 notifier 落盘后、outbox 对账前退出，下次运行会执行第 2 步并移除 pending，不再重发。若在传输成功后、notifier 落盘前退出，仍可能重复一条消息；这是可接受的至少一次边界。

持仓浮盈亏变化相对“上次已确认浮盈亏”计量，而不是相对“上一分钟观察值”，避免一连串小亏损不断挪动基线、绕过累计阈值。

所有读-改-写操作在加载状态前获取 advisory 文件锁，并持有到原子替换完成，与仓库现有 latest-state 并发模式一致。

### 6.3 新鲜度与损坏行为

默认最大持仓快照年龄为 `max(3 * poll_interval_seconds, 180 seconds)`，可配置。重复快照可重放 pending 事件，但不派生新事件。

快照拒绝只影响新事件派生。即使当前快照缺失、过期、不完整或非单调，已有 pending 事件仍会对账并重试。

事件库 JSON 无效时 fail closed：发出运维错误，不派生开/平仓事件，也不用空基线覆盖文件。版本 1 状态迁移时，将其旧数量与账面浮盈亏视为初始已观察/已确认基线，避免部署时误发历史开仓告警。

## 7. 报价新鲜度与使用策略

### 7.1 两个独立事实

行情模式与新鲜度不是一回事：

- 行情模式：live、frozen、delayed、delayed-frozen；
- 传输新鲜度：源观察最近一次推进的时间。

`Quote` 增加可选字段 `last_update_at`。对持久 IBKR 订阅，`snapshot_rows` 仅在 ticker 时间或实质性 ticker 指纹推进时更新它；再次写入同一缓存行不会刷新。这样，天然延迟约 15 分钟的行情只要持续更新，仍可保持传输新鲜。

归一化指纹包含：ticker 时间、bid、ask、last、市价、收盘价、bid/ask/last 量、成交量、持仓量、model IV、delta、gamma、model 标的价。首次有效观察设置 `last_update_at`；之后若所有归一化字段完全相等则不推进。比较一律使用带时区的 UTC 时间戳与清洗后的有限数值。

无 `last_update_at` 的旧行仍可读，但无法证明可执行。live 行可用源报价时间回退判断新鲜度；delayed 旧行仅作研究用，新鲜度标为 unknown。

### 7.2 中央判定

统一 helper 返回：

```text
feed_mode
freshness = fresh | stale | unknown
research_usable
alert_allowed
pricing_allowed
reason
```

策略：

- 新鲜 live 数据可用于研究、告警、定价。
- 新鲜 frozen 数据仅在调用方显式允许相关收盘参考时可用。
- 新鲜 delayed / delayed-frozen 数据仅可作为带标签的研究上下文。
- stale / missing / error / unknown 数据不得驱动告警或定价。
- delayed 行情若超过其配置的传输阈值仍不推进，则无论 market-data 类型如何，都变为 stale。

默认传输阈值：热标的 15 秒；delayed / delayed-frozen 研究行情 60 秒；配置的慢标签 300 秒。环境变量可覆盖各项；慢标签阈值优先于行情模式默认值。当 `as_of - last_update_at <= threshold` 为 fresh，大于则为 stale。时间戳超前超过 5 秒则 freshness=unknown，并 fail closed。

`alert_engine`、`market_context`、`human_focus`、`order_map` 与持仓 watcher 都消费该 helper，不再各自维护 divergent 的坏质量集合。现有 options-map 对 delayed 数据的排除继续强制执行。

## 8. ATM 参考与期权计划稳定性

### 8.1 参考溯源

`AtmReferenceController` 返回结构化候选：

```text
value
rounded_strike
source
observed_at
freshness
basis_value
basis_as_of
basis_contract
reason
```

来源策略：

1. RTH 内，新鲜 SPX 为权威。
2. 非 RTH，优先使用可用的新鲜现金级 IBUS500 报价。
3. ES 仅在存在持久化 basis 证据时可做 basis 调整；该证据须在 RTH 内、SPX 与同一 ES 合约同时新鲜时观察到。
4. 其次回退为新鲜 SPY×10。
5. 仅换到期日时，可复用上次稳定 ATM。
6. 过期 SPX 收盘价仅允许在无控制器状态且无新鲜代理时做一次性 bootstrap；不得引发后续由源驱动的重规划。

删除当前 `ES.close - SPX.close` 捷径。basis 样本仅在 RTH 内、SPX 与同一 ES 合约均新鲜、且源观察时间差不超过 5 秒时接受。控制器维护滚动 5 分钟样本窗，至少 5 个样本且跨度至少 30 秒；拒绝绝对 basis 超过 120 点；拒绝相对当前中位数偏离超过 15 点的新样本；持久化中位数而非单 tick。有效 basis 状态含 ES 合约月、交易日、样本窗、数量、中位数、观察时间。ES 合约月变化时失效，并在 3 个美股交易日后过期。RTH 内新鲜 SPX 立即覆盖它。

控制器状态以原子方式、权限 `0600` 持久化，避免服务重启丢失有效 basis 或上次稳定 ATM。

### 8.2 重规划控制器

默认策略：

- 触发带：接受 ATM 与计划 ATM 相差至少 20 点；
- 复位带：差值回到至多 10 点；
- 正常确认：同一取整 ATM 与来源至少 3 次观察，跨度至少 15 秒；
- 来源宽限：短暂失去新鲜度最多 30 秒内保留当前来源；
- 正常重建最小间隔：120 秒；
- 紧急移动：至少 40 点且两次一致观察；
- 到期日变化：立即执行，豁免冷却。

显式状态机：

- `steady`：无活跃候选；相差至少 20 点进入 `pending`。
- `pending`：同一取整 ATM 与来源累计确认。来源或取整行权价变化则重置确认窗；回到至多 10 点则取消并回 `steady`。
- `cooldown`：成功重规划后进入。120 秒内忽略正常候选，之后开启新确认窗。

同源 40 点紧急候选可在至少 5 秒跨度的两次观察后绕过 120 秒冷却，但任意两次行情驱动重规划之间仍有硬性 30 秒下限。到期日换日绕过两种冷却。`accepted` 参考与来源仅在初始规划或重规划成功后变更，不会仅因观察到原始候选而变更。

当前原始 ATM 缺失时，到期日变化仍会发生：把上次稳定 ATM 带入新到期日。对每个新计划 key 恰好发生一次。

每次决策日志记录：原始与接受参考、溯源、pending 确认次数、basis 证据、决策原因，以及保留/新增/移除合约数。

### 8.3 订阅对账

重规划使用合约集合 diff：

1. 保留旧/新热集合交集。
2. 仅在需要腾出线路容量时释放过时的轮换或远尾合约。
3. 订阅并 qualify 新增热合约。
4. 仅在替换成功后移除剩余过时热合约。
5. 用剩余预算重建轮换切片。

若新订阅失败，保留覆盖继续活跃，控制器退避，而不是反复重建。SPY 有独立计划 key；到期日与取整 ATM 未变时不重建。

采集器对合约 qualify 使用一种受支持的同步风格；不得创建未 await 的 `qualifyContractsAsync` 协程。

期权缓存保留活跃采样计划允许的每个到期日（含下一研究到期日），直到 TTL 或真实到期换日移除。

## 9. 协作式慢轮询

`SlowPollScheduler` 有两个状态：

- `idle`：无临时订阅；
- `holding`：已订阅一个 chunk，直到 hold 截止。

每个正常 stream 迭代推进一次：

1. idle 且到期时，订阅一个 chunk 并记录截止时间。
2. 继续正常的 5 秒热路径 flush。
3. holding 且截止已过时，快照、取消并缓存该 chunk，不 sleep。
4. 调度下一个 chunk。

chunk 在配置周期内均匀铺开；19 个标签、chunk 大小 6、周期 300 秒时，四个 chunk 之一大约每 75 秒启动。合约在每个 IBKR 会话 qualify 一次，后续周期复用；重连开启新的 qualify 缓存。

chunk 错误只取消该临时 chunk，记录降级慢车道事件，并带退避重试。除非底层 IBKR 会话本身丢失，否则不阻塞或拆掉热路径 stream。

## 10. Hyperliquid：研究 vs 可执行定价

`resolve_spx_spot` 替换为 `SpotResolution`：

```text
research_price
research_source
pricing_price
pricing_source
pricing_allowed
gate_state
reason
divergence_bps
```

研究参考价在现金时段外可用 Hyperliquid。定价参考价必须通过现有 market-context 锚定与 basis 门控，且来自可执行 TradFi 或期权链证据。

当 Hyperliquid 是唯一可用参考，或状态为 `unanchored` / `basis_warn` / `basis_blocked` 时：

- payload 仍有效，并显式 `research_only=true`；
- 可展示 HL 价格、方向上下文、墙距、情景行权价、观察到的期权 bid/ask；
- `pricing_allowed=false`；
- 可执行限价字段、模型重定价、触达概率、触达 ETA 均为 null，且不得表述为建议。

存在有效 TradFi 或期权链锚时，候选与限价计算只使用 `pricing_price`。Hyperliquid 仅作确认或背离字段，不能替换该价格。

order-map payload 暴露独立的 `research_reference` 与 `pricing_reference`。当 `pricing_allowed=false` 时，兼容字段 `underlier.price` / `underlier.source` 为 null，旧版 `candidates` / `wall_ladder` 为空，其他可执行数值别名均为 null。HL 派生的情景行权价、观察报价与墙距只放在 `research_candidates` / `research_wall_ladder`。所有候选选择、重定价、概率、ETA、日内波幅函数在计算前都要求 `pricing_allowed` 决议；这是模型层门控，不是渲染标签。合法的 research-only 地图不视为 thin 或失败 payload。非常规时段的定期研究状态仍可渲染新研究字段并带 research-only 标签；不会变成直接可执行告警。

## 11. 统一市场日历与 17:00 换日

`market_calendar.py` 成为唯一来源，负责：

- 美股交易日判断；
- 已观察的全日假期；
- 耶稣受难日与六月节；
- 标准 09:30–16:00 ET 时段；
- 计划中的 13:00 ET 提前收盘；
- 上一/下一交易日；
- RTH 开盘判断；
- 已完成复盘日期；
- 当前与下一研究到期日。

实现使用确定性日历规则 + 例外休市显式覆盖，不引入大型市场日历依赖。

研究到期日规则：

- 交易日 17:00:00 ET 之前用当日；
- 17:00:00 ET 及之后用下一交易日；
- 周末或全日假期始终用下一交易日；
- 第二到期日为研究到期日之后的下一个交易日。

提前收盘改变时段窗口与报告覆盖分母，但用户批准的研究换日仍固定在 17:00 ET。

`default_spxw_expiry` 保留为兼容包装。采样、stream-collector 到期换日、options map、order map、alert profile、现金时段检查、盘后复盘全部委托给该日历。

盘后就绪时间改为 17:00 ET。timer 在工作日 `America/New_York` 17:15 运行，与服务器时区及夏令时无关。应用仍检查假期与报告身份，避免假期 timer 重发上一份报告。

## 12. 盘后完备性

`ReviewCompletenessPolicy` 评估结构化检查。每项记录测量值、阈值、通过/失败、原因。状态保持向后兼容：

- 仅当所有必需检查通过时为 `complete`；
- 否则为 `degraded`。

按实际时段长度的默认必需检查：

- SPX 与 ES 各自覆盖至少 90% 的预期 5 分钟 RTH 桶。
- 首个可用 SPX/ES 观察在开盘后 15 分钟内。
- 末个可用 SPX/ES 观察在实际收盘前 15 分钟内。
- SPX 与 ES live 比例至少 95%。
- 该交易日 SPXW 到期至少有 20 个唯一合约、10 个行权价、同时有 call 与 put，行权价跨度至少 50 点。
- 至少 90% 期权行为可用的 live/frozen RTH 观察。
- 期权 IV 覆盖至少 80%，且收盘前 15 分钟内存在近月期权观察。
- 近月 IV 曲面覆盖至少 60% 预期 5 分钟桶，并在收盘前 15 分钟内结束。
- 最新近月 IV 与 gamma 覆盖率各自至少 50%。

阈值由策略对象表示，可通过校验后的环境变量覆盖。报告 JSON 与 Markdown 展示检查项与警告。单行合成样例与当前 2026-07-08 低覆盖样例必须为 degraded。

## 13. 迁移与部署行为

- 现有报价 JSON 仍可读，因为 `last_update_at` 可选。
- 版本 1 持仓状态迁移时不生成历史事件。
- 缺失 ATM 控制器状态时做一次初始规划，随后进入正常稳定。
- 无关的未跟踪文件 `earlyoom_1.7-2_arm64.deb` 保持不动。
- 代码在隔离 worktree 中实现并测试，再集成。
- 静态与回放验证后，按受控顺序重启相关用户服务。线上 soak 只读，不下单。

## 14. 测试与验证设计

实现以失败优先、测试驱动。

### 持仓测试

- 数量 `+2` / `-2`，`avgCost=3200`，mark `25`，分别得到 `-1400` / `+1400`。
- 账面成本对每条腿乘以绝对数量。
- 覆盖合约乘数与仅 SPXW 回退。
- 完整空快照会平仓；不完整或过期空快照不会。
- sink 失败与 `--no-notify` 保留同一 pending 事件 ID。
- 一个成功 sink 只确认已投递事件 ID。
- 重启对账处理“notifier 已落盘但 outbox 仍 pending”的事件。
- stale / delayed mark 不得发出 live 浮盈亏告警。
- 快照与事件文件权限为 `0600`。

### 新鲜度测试

- delayed 报价源时间天然晚约 15 分钟，但源观察持续推进时，仍为新鲜的 research-only 数据。
- 同一行情在传输更新停止后变为 stale。
- delayed 数据不得触发行情告警、可执行期权定价或持仓浮盈亏告警。
- 现有 options-map delayed 排除保持通过。

### ATM 与 stream 测试

- 回放 `7480 -> 7510 -> 7480 -> 7510 -> 7545 -> 7480`，不反复重规划。
- 错配收盘证据不得生成错误的 7480 basis 调整参考。
- 同源持续移动恰好触发一次重规划。
- 短暂 30 秒来源中断不触发重规划。
- 17:00 到期换日且无原始 ATM 时，恰好一次使用稳定 ATM。
- 订阅 diff 保留交集，并在部分新增失败时存活。
- 未变的 SPY 计划不再次 qualify。
- 慢轮询假时钟测试不含 10 秒阻塞 sleep，最终覆盖全部标签，并保持热路径 flush 节奏。
- 测试与线上日志均无未 await 的 qualify 协程警告。

### 日历与报告测试

- 周四 16:59 ET 用周四；17:00 用周五。
- 周五 17:00 用周一。
- 2026-07-02 17:00 用 2026-07-06，跳过已观察的 7 月 3 日假期与周末。
- 覆盖耶稣受难日、感恩节、跨年观察假期、提前收盘、DST timer 行为。
- 单行报告 fixture 为 degraded。
- 当前 2026-07-08 证据因近月 IV/gamma 覆盖低于 50% 而为 degraded。
- 全时段、广覆盖、高质量 fixture 仍为 complete。

### 质量门禁

最终本地等价 CI 命令：

```text
uv run ruff check .
uv run pytest -q
systemd-analyze verify systemd/*.service systemd/*.timer
```

GitHub Actions 在 Python 3.12 上跑 Ruff 与 pytest。日历 timer 语法在 Oracle 主机上单独验证。

生产验收包含 60 分钟只读 stream soak：

- 无来源来回跳变式重规划；
- 每次重规划都有可审计原因；
- 慢轮询下热路径 flush 最大间隔保持在 12 秒内；
- 无新增 IBKR pacing、会话或未 await 协程错误；
- 持仓事件 dry-run 证明 pending 保留、稳定事件 ID 与重放，且不暴露账户标识。

按事件确认由集成测试证明，使用确定性假飞书/Bark 成功与失败结果。生产 dry-run 永不确认事件。受控真实 sink 测试可选，仅在部署验证显式开启时执行。

## 15. 完成标准

仅当上述每项要求都有直接证据时，本批次才算完成：

- 持仓计算与事件可靠性测试通过；
- 新鲜度与可执行性门控通过；
- ATM 回放、期权计划 diff、慢轮询测试通过；
- Hyperliquid 仍可作为非常规时段研究参考，但不能成为唯一可执行定价锚；
- 17:00 ET 与所有日历消费者一致；
- 盘后完备性检查正确分类稀疏与低质量交易日；
- pytest、Ruff、workflow CI、systemd 校验、回放检查与线上 soak 全绿。
