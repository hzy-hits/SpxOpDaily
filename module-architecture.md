# SPX Spark 模块架构与分层协议

状态: 2026-07-07 起生效。本文档是模块划分的唯一权威来源;新增模块或 import 前先对照本文分层规则。
配套守护测试: `tests/test_architecture.py`(见下文 §5,违反分层规则会直接挂测试)。

## 1. 分层总览(低层在下,依赖只允许指向同层或更低层)

```
L5 orchestration   service_loop, maintenance, post_close_review, latest_state
L4 alerting        alert_engine, notifier/*, position_alerts, alert_profile(窗口定义)
L3 analytics       options_map, iv_surface, market_context, human_focus, strategy/*
L2 providers       ibkr/*, schwab/*, hyperliquid/*, polymarket/*, mock_collector
L1 infrastructure  storage, sampling, runtime_mode, provider_adapter
L0 foundation      config, marketdata, alert_model
```

分层规则(守护测试强制执行):

1. 任何模块只能 import 同层或更低层的模块。
2. L0 三个模块不得 import 任何 spx_spark 内部模块(彼此之间也不行,保持零依赖)。
3. provider 包(L2)之间不得互相 import(ibkr 不能 import schwab 等)。
4. 只有 L5 orchestration 和测试可以跨层拼装(import 任意层)。
5. 非 provider 模块不得 import provider 包,例外:L5。
   (alert_engine 需要的持仓告警逻辑在 L4 的 `position_alerts`,它 import
   `ibkr.position_watcher` 属于 L4→L2 向下依赖,合法。)

## 2. 各层职责与模块清单

### L0 foundation(零内部依赖)
- `config.py` — 全部 Settings dataclass + `from_env()`;公共 env 读取工具
  `env_str/env_int/env_float/env_bool/env_csv/env_csv_preserve`(其他模块一律从这里
  import,禁止自己写 `os.getenv` 解析布尔/浮点)。
- `marketdata.py` — 纯领域模型:`InstrumentId/Quote/OptionGreeks/Provider/
  MarketDataQuality/ProviderState` + 通用归一化工具(`clean_float/parse_timestamp/
  classify_quote_quality/as_utc/elapsed_ms/bool_or_none` 等)。
  **不含任何 provider 专有转换**(IBKR/Schwab 字段名知识分别在各自 adapter)。
- `alert_model.py` — `Alert` dataclass + `severity_for_priority`。告警的生产者
  (alert_engine、position_alerts)与消费者共用,避免反向依赖。

### L1 infrastructure
- `storage.py` — JSONL 写入、`LatestState/LatestStateStore`。
- `provider_adapter.py` — `ProviderSnapshot` 与快照持久化协议;provider 无关。
- `sampling.py`、`runtime_mode.py`。

### L2 providers(每个包自治:连接 + 原始数据 + 归一化 adapter)
- `ibkr/verifier.py` 传输层(IB API 调用、订阅、VerifyRow);
  `ibkr/adapter.py` 归一化(`quote_from_ibkr_row/instrument_from_ibkr_label/
  CFD_UNDERLIERS/snapshot_from_rows`);collector/stream_collector/farm_health/
  position_watcher/gateway/trading_hours_report。
- `schwab/adapter.py` 归一化(`quote_from_schwab_payload/quote_from_schwab_option_contract/
  instrument_from_schwab_symbol` + `first_key/nested_mapping/parse_expiry`);verifier、token_helper。
- `hyperliquid/`、`polymarket/`、`mock_collector`。
- 协议:provider 对外交付物统一为 `ProviderSnapshot`(L1)与 `Quote`(L0);
  上层不接触 provider 原始 payload。

### L3 analytics(输入 LatestState/Quote,输出结构化分析)
- `options_map.py`(OI/gamma/wall)、`iv_surface.py`、`market_context.py`、
  `human_focus.py`(人类关注上下文,含 vol_context)、`strategy/micopedia.py`。

### L4 alerting
- `alert_profile.py` — 时段窗口定义。
- `alert_engine.py` — 告警评估(从 L3/L1 读取,产出 `Alert`,调用 notifier)。
- `position_alerts.py` — SPXW 持仓告警(从 `ibkr/position_alerts.py` 迁入,见 §4B)。
- `notifier/` — 通知管道包(见 §4A)。

### L5 orchestration
- `service_loop.py`、`maintenance.py`、`post_close_review.py`、`latest_state.py`。

## 3. 已完成的重构(记录,便于回溯)

- `Alert`/`severity_for_priority` 从 alert_engine 抽到 `alert_model.py`;
  env 工具公有化到 config;`ibkr/position_alerts` 不再 import alert_engine(曾是 L2→L4 反向依赖 + 靠 lazy import 掩盖的循环)。
- `quote_from_ibkr_row/instrument_from_ibkr_label/CFD_UNDERLIERS` 移入 `ibkr/adapter.py`;
  `quote_from_schwab_*`/`instrument_from_schwab_symbol`/`first_key/nested_mapping/parse_expiry`
  移入 `schwab/adapter.py`;marketdata 成为纯领域模型。
- 通知双通道:微信(openclaw)+ Bark;agent 只分析不直投(无 `--deliver`),
  投递由 gate 判定;被否决的告警进冷却。

## 4. 变更规格(2026-07-07 已全部实施完毕;保留原文供回溯)

实施记录:§4A/§4B/§4C 由实施 agent 完成,守护测试随即抓到一条既有违例
`alert_engine -> ibkr.position_watcher`(position_holdings_alerts 里的延迟 import)。
裁决:不加白名单,把 `position_holdings_alerts` 整体下沉到 `position_alerts.py`,
alert_engine 只 import `position_holdings_alerts` 一个入口。213 测试全绿。

### 4A. notifier.py(896 行)拆为包 `src/spx_spark/notifier/`

新文件与函数归属(全部从现 `notifier.py` 原样搬移,不改逻辑、不改函数签名):

| 新文件 | 内容(现 notifier.py 中的符号) |
|---|---|
| `model.py` | `SinkResult`、`NotificationResult`、`CommandRunner` 类型别名、`default_runner` |
| `policy.py` | `SEVERITY_RANK`、`POSITIVE_DELIVERY_CUES`、`NEGATIVE_DELIVERY_CUES`、`HUMAN_VISIBLE_ALERT_PREFIXES`、`BLOCKED_HUMAN_MESSAGE_SYMBOLS`、`BLOCKED_HUMAN_MESSAGE_PHRASES`、`SYSTEM_EVENT_ALERT_KINDS`、`POSITION_HOLDING_ALERT_KIND_PREFIX`、`POSITION_HOLDING_SOURCE_GATE`、`POSITION_DIRECT_PUSH_KINDS`、`severity_value`、`alert_key`、`is_human_visible_alert`、`is_system_event_alert`、`is_position_holding_alert`、`direct_push_alerts`、`codex_message_requests_delivery`、`codex_message_respects_human_scope`(连同 IV-surface 走审阅路径的注释一起搬) |
| `state.py` | `load_sent_state`、`save_sent_state`、`select_alerts_for_notification`、`mark_alerts_sent`(import policy 的 `severity_value/alert_key/is_human_visible_alert`) |
| `prompts.py` | `format_alert_message`、`build_agent_prompt`、`compact_window`、`compact_analysis_payload`、`build_codex_prompt` |
| `sinks.py` | `openclaw_state_dir`、`resolve_default_weixin_delivery`、`run_codex_exec`、`send_openclaw_message`、`openclaw_delivery_error`、`openclaw_payload_error`、`extract_openclaw_agent_message`、`run_openclaw_agent`(保留其 docstring:解释为什么绝不带 `--deliver`)、`post_bark`、`send_bark_message`、`bark_title_for_alerts` |
| `pipeline.py` | `notify_payload`(逻辑一字不改) |
| `__init__.py` | 见下 |

包内依赖方向(必须遵守,禁止反向):
`model.py` ← `policy.py` ← `state.py`/`prompts.py`/`sinks.py` ← `pipeline.py` ← `__init__.py`
(`model.py` 与 `policy.py` 只依赖 stdlib + `spx_spark.config`。)

`__init__.py` 内容:模块 docstring(一句话:通知管道,选取→审阅→gate→双通道投递)+
re-export 公共 API,并定义 `__all__`:

```python
from spx_spark.notifier.model import (
    CommandRunner, NotificationResult, SinkResult, default_runner,
)
from spx_spark.notifier.pipeline import notify_payload
from spx_spark.notifier.policy import (
    alert_key, codex_message_requests_delivery,
    codex_message_respects_human_scope, direct_push_alerts,
    is_human_visible_alert, severity_value,
)
from spx_spark.notifier.prompts import build_codex_prompt, format_alert_message
from spx_spark.notifier.sinks import (
    openclaw_delivery_error, run_codex_exec, run_openclaw_agent,
    send_bark_message, send_openclaw_message,
)
from spx_spark.notifier.state import (
    mark_alerts_sent, select_alerts_for_notification,
)
```

注意事项:
1. 先 `git rm` 不需要——直接删除 `src/spx_spark/notifier.py` 并新建目录(同名 module→package 替换)。
2. `tests/test_notifier.py` 两处 `monkeypatch.setattr("spx_spark.notifier.post_bark", ...)`
   改为 `"spx_spark.notifier.sinks.post_bark"`(pipeline 里调用的 `send_bark_message`
   解析的是 `sinks` 模块全局)。其余 `from spx_spark.notifier import ...` 不需要动。
3. 各新文件头部按需保留 `from __future__ import annotations` 与最小 import 集;
   跑 `uv run ruff check src/ tests/` 清理未用 import。

### 4B. `ibkr/position_alerts.py` 迁到 `src/spx_spark/position_alerts.py`

原因:它是告警逻辑(L4),不是 IBKR 传输(L2);现位置迫使"L2 import options_map(L3)"违例。
- `git mv src/spx_spark/ibkr/position_alerts.py src/spx_spark/position_alerts.py`,内容不改
  (它 import `ibkr.position_watcher` 是 L4→L2 向下依赖,合法)。
- 更新两个引用处:
  - `alert_engine.py` 内的延迟 import:`from spx_spark.ibkr.position_alerts import ...`
    → `from spx_spark.position_alerts import ...`(现在可以改成模块顶层 import,
    原先 lazy 是为了绕开已不存在的循环依赖;顶层 import 后请验证无循环)。
  - `tests/test_ibkr_position_alerts.py` 的 import;文件同时改名为
    `tests/test_position_alerts.py`。

### 4C. 新增架构守护测试 `tests/test_architecture.py`

用 stdlib `ast` 扫描 `src/spx_spark` 全部 .py,提取 `spx_spark.*` 的顶层 import
(含 `from spx_spark.x import y`;函数体内的延迟 import 也要抓——遍历整棵树而非只看
module body),按下表判层,断言:每个模块 import 的目标层 ≤ 自身层;L5 除外(任意)。

```python
LAYERS = {
    "config": 0, "marketdata": 0, "alert_model": 0,
    "storage": 1, "sampling": 1, "runtime_mode": 1, "provider_adapter": 1,
    "ibkr": 2, "schwab": 2, "hyperliquid": 2, "polymarket": 2, "mock_collector": 2,
    "options_map": 3, "iv_surface": 3, "market_context": 3,
    "human_focus": 3, "strategy": 3,
    "alert_profile": 4, "alert_engine": 4, "notifier": 4, "position_alerts": 4,
    "service_loop": 5, "maintenance": 5, "post_close_review": 5, "latest_state": 5,
}
```

判层规则:取模块相对 `spx_spark` 的第一段名字(如 `ibkr.adapter` → `ibkr`)。
额外三条独立断言:
1. L0 模块不 import 任何 `spx_spark.*`。
2. L2 provider 包互相不 import(第一段名字不同的两个 L2 包之间)。
3. 除 L5 与 L2 自身外,任何模块 import L2 时只允许目标为
   `spx_spark.position_alerts` 允许的 `ibkr.position_watcher`(白名单常量写死,
   新增白名单必须在本文档 §1 登记理由)。

测试输出要求:失败信息里列出 `违例模块 -> 被import模块 (层X -> 层Y)`,方便定位。

### 4D. 验收标准(实施 agent 自检后交付)

1. `uv run pytest -q` 全绿(现有 212 个 + 新架构测试)。
2. `uv run ruff check src/ tests/` 无错误。
3. `rg -n "from spx_spark.notifier import" src/` 只出现 package 级 import。
4. `python -c "from spx_spark.notifier import notify_payload"` 可运行。
5. 不改任何运行时行为:除 import 路径外,函数体零改动(diff 里搬移的代码块应逐字一致)。

## 5. 日常约定

- 新模块先在 §1 表里定层,再写代码;守护测试挂了不许改测试白名单蒙混,先回来改设计。
- provider 字段名知识只允许出现在对应 `*/adapter.py`。
- 跨层共享的数据结构一律下沉到 L0(参考 `alert_model.py` 的先例)。
