# 通知消息契约与模板协议

<!-- documentation-status: 2026-10-18 -->
> **文档定位：现行设计合同（内容协议部分待实施）。** 投递链路已经是生产实现；
> 本文定义消息内容如何从“字符串”升级为“事实 + 模板”。
> [全仓文档目录](README.md) · [通知架构](notification-architecture.md) ·
> [决策协议](architecture/decision-protocol.md)

## 1. 已经可用的部分

投递链路本身不需要重做：

- `NotificationEnvelope`：`event_id` / `source` / `kind` / `lane` / `occurred_at` /
  `expires_at` / `operator_targets` / `opportunity_id` / `generation`；
- 状态机：`pending` → `processing` → `delivered` / `failed` / `uncertain`，
  每个 target 独立 attempt；
- 幂等、取消、启动恢复、dead-letter、每目标回执；
- lane 语义：`ops_transition` / `market_warning` / `trade_ready` /
  `position_safety` / `scheduled_report`；
- Bark / 飞书 transport 与重试。

结论：**能发、能重试、能证明送达**。

## 2. 不达标的部分

1. 内容是 `title / text / feishu_text` 三个字符串，没有字段结构；
2. 飞书卡片的分区是**解析 Markdown** 得到的（`format_push.py` 按行切分，
   `_COLLAPSED_STATUS_SECTIONS` 硬编码），不是结构化模板；
3. 没有 template id / version，没有每类消息的 golden 样例；
4. LLM 可以改写正文，数字没有 schema 约束，只能靠 prompt；
5. Python notifier 与 Rust delivery 各自渲染，模板会漂移；
6. 没有“已读/已确认”回执，只有 transport receipt。

## 3. MessageIntent（事实）

现状（2026-10-18）：`NotificationEnvelope` 已带 `message_class`、`template_id`、
`template_version`，lane→class 有默认映射；人工交易卡已标注 `ready` / `watch`。
模板注册表与数字锁定仍是目标。

```python
@dataclass(frozen=True)
class MessageIntent:
    message_id: str
    message_class: str      # watch|armed|ready|no_trade|execution|management|exit|post_mortem|ops|data_quality
    lane: str
    severity: str
    as_of: datetime
    expires_at: datetime
    subject: Subject        # instrument / structure / legs
    decision: DecisionFacts # status, reasons, policy_version, model_version, decision_id
    economics: Economics | None   # entry_basis, max_loss, target, ev, pop, sample_n, calibration
    risk: RiskFacts | None        # invalidation, stop, time_stop, hard_exit
    data_quality: DataQuality     # provider, quote_age, freshness, degraded_fields
    provenance: Provenance        # snapshot_id, correlation_id, projection_id
    narrative: str | None         # LLM 只填这段，不得改数字
```

字段规则：

| 字段 | 规则 |
| --- | --- |
| `message_class` | 固定枚举；未知值 fail closed |
| `as_of` / `expires_at` | aware UTC；过期消息不得发送 |
| `subject.structure` | `CALL_DEBIT_VERTICAL` / `PUT_DEBIT_VERTICAL` / `IRON_CONDOR` / `BUTTERFLY` 等 |
| `economics` | 每个数字必须来自 decision facts；`sample_n` 与 `calibration` 必填 |
| `data_quality` | 分别记录 BBO / Greeks / OI 的时钟，不只一个 `fresh` 布尔 |
| `narrative` | 只能描述，不能引入新数字；渲染前做数字白名单校验 |

## 4. 消息类别

| 类别 | 受众 | 内容 | 允许的动作语言 |
| --- | --- | --- | --- |
| `watch` | trader | 观察中的结构/条件 | 无 |
| `armed` | trader | 触发条件已满足，等证据 | 无（带 `evidence_age`） |
| `ready` | trader | 人工候选：腿/价格/风险/失效 | “可人工决策”，不是“下单” |
| `no_trade` | trader | 不建立新风险及主要原因 | 无 |
| `execution` | trader | 入场价、限价、失效、有效期 | 人工执行提示 |
| `management` | trader | mark、目标、止损、时间止损 | 管理建议 |
| `exit` | trader | 退出原因与结果 | 事实 + 复盘 |
| `post_mortem` | trader | 当日机会/结果/偏差 | 复盘 |
| `ops` | operator | 健康、gap、回执、部署 | 运维动作 |
| `data_quality` | operator | 缺失字段、过期时钟 | 只描述 |

`ops` 与 `ready` 严禁混用：决策卡不写“服务重启”，运维卡不写“建议入场”。

## 5. 模板协议

```python
@dataclass(frozen=True)
class Template:
    template_id: str       # "trade_ready.feishu"
    version: int
    channel: str           # bark | feishu | web
    message_class: str
    render(intent: MessageIntent) -> str | dict
```

规则：

1. 模板只读 `MessageIntent`，不解析 Markdown；
2. 每个 `(template_id, channel)` 一份 golden 输出，改动必须 bump version；
3. Bark 版可以是短摘要，但必须包含：结构、入场依据、max loss、失效、有效期；
4. 飞书卡片分区固定，不由 LLM 决定；
5. LLM 只填 `narrative`；失败时用确定性模板，不阻塞投递；
6. 数字锁定：渲染前提取 narrative 中所有数字，必须能在 `economics/risk` 找到，
   否则丢弃 narrative 并记录 `narrative_number_mismatch`。

## 6. 量化机构视角：决策消息必须回答什么

1. **做什么**：结构、方向、strike、width、expiry、数量；
2. **凭什么**：entry basis（bid/ask/mid 哪个）、quote age、provider；
3. **亏多少**：max loss、stop、time stop、hard exit；
4. **胜率/赔率**：EV/PoP、样本 n、样本窗口、是否校准（未校准必须写）；
5. **什么时候失效**：invalidation 条件、`valid_until`；
6. **数据可信吗**：BBO/Greeks/OI 各自时钟、缺失字段；
7. **怎么追溯**：decision_id、policy_version、model_version、snapshot_id；
8. **一句话结论**：可读，但不得改数字。

普通消息（ops）只回答：哪个服务、什么状态、从什么时候开始、影响了什么、
恢复了没有、回执如何。

## 7. 投递与确认

- 保留现有 outbox / attempt / receipt 表，不新建队列；
- `notification_intent` 增加 `template_id/version` 与 `message_class`；
- transport receipt 之外，增加可选的 `acknowledged_at`（人工确认），
  与 receipt 分开存储；
- 同一 `opportunity_id + generation + message_class` 只允许一条有效消息，
  编辑/更正用新 generation。

## 8. 迁移顺序

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P0 | 定义 `MessageIntent` 与类别，不改发送 | 现有消息能映射到类别 |
| P1 | 决策消息改用结构化事实渲染 | 飞书分区不再解析 Markdown |
| P2 | 模板 registry + golden | 模板改动必须 bump version |
| P3 | narrative 数字校验 + 可选 ack | 不匹配时回退确定性模板 |

## 9. 非目标

- 不改变 Bark/飞书 transport、重试、幂等和 owner；
- 不新增常驻服务、不引入模板引擎依赖；
- 不把通知记录当策略样本或收益标签。
