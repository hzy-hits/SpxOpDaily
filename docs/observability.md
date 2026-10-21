# 运行观测与决策延迟指标

<!-- documentation-status: 2026-10-19 -->
> **文档定位：现行设计合同（指标投影部分待实施）。** 定义“怎么知道系统还好、
> 决策有多快、消息有没有送到”，不引入新常驻服务。
> [全仓文档目录](README.md) · [决策协议](architecture/decision-protocol.md) ·
> [服务清单](service-inventory.md)

## 1. 现状

| 已有 | 位置 | 局限 |
| --- | --- | --- |
| 结构化日志 | `logging_setup.py`（structlog） | 只有入口配置，业务代码大量用 `print(json.dumps)` |
| 阶段耗时 | `market_features/service.py` 的 `stage_durations_ms` | 只打到 stdout，没有聚合 |
| 健康文件 | `latest/*_health.json`、`ibkr_stream_health.json` | 各模块自定 schema，无统一读取方 |
| HTTP 健康 | Schwab FastAPI `/healthz` | 只覆盖 OAuth/网关 |
| 业务台账 | `spx.sqlite` events/decisions/outcomes/notification | 事实齐全，但不是指标 |
| 回执 | Python/Rust delivery receipt | 能证明送达，不能证明延迟分布 |
| Fast lane | `latest/fast_lane.json` | 1s 投影，带 latency/evidence_age，无聚合与告警 |
| 故障监测 | Worker 每分钟 `monitor_desk_pipeline` | 只覆盖桌图链路 |

缺口：没有 correlation_id 的日志检索、没有延迟/新鲜度的聚合指标、没有 SLO 告警、
没有“决策变慢”的告警。

## 2. 五个金信号（按 lane）

| 信号 | 定义 | 数据来源 | 阈值示例 |
| --- | --- | --- | --- |
| Freshness | 最新 `source_at` 距 now | `latest` 投影、health | 超 `latest_stale_after_seconds` 告警 |
| Latency | detected→ready、ready→receipt | `stage_durations_ms`、DB 时间戳 | P95 > 3s 告警 |
| Errors | 失败/超时/拒绝率 | logs、DB outcome | 连续 3 次失败告警 |
| Saturation | 决策周期耗时 / 周期预算 | stage durations | > 60% 预算 warning |
| Delivery | 入队→回执、uncertain 数 | notification events/receipts | uncertain > 0 告警 |

## 3. 最小实现：ops snapshot 投影

每个常驻 owner 每 60s 写一个 `latest/ops_<service>.json`（原子替换）：

```json
{
  "schema_version": 1,
  "service": "core",
  "observed_at": "...",
  "window_seconds": 60,
  "decision_cycles": 12,
  "decision_latency_ms": {"p50": 180, "p95": 420, "max": 900},
  "stage_durations_ms": {"state_and_options": 35, "research_and_strategy": 380},
  "freshness": {"spx_source_age_s": 2.1, "option_quote_age_s": 4.8},
  "errors": {"count": 0},
  "gaps": {"ibkr_stream": "healthy"}
}
```

规则：

1. 只追加事实，不做跨进程聚合；
2. `correlation_id` 写入所有 decision/notification 日志与行；
3. Worker 的监测任务读取这些快照和 receipts，生成一条 ops 消息；
4. 不引入 Prometheus/OTel 依赖；如果以后需要，快照字段就是导出的基础。

## 4. 决策延迟的度量口径

- `decision_latency_ms`：目标为从 cycle 开始到 `strategy_decision` 可发布；market_features
  目前只在内部局部计算，未输出到 cycle summary，指标接线仍待实施；summary 已有阶段
  耗时。fast lane 的 `latest/fast_lane.json` 输出自己的
  `latency_ms` / `evidence_age_ms`；
- `evidence_age_ms`：决策使用的报价/Greeks 年龄；
- `time_to_ready`：detected→authorized（机会台账）；
- `time_to_deliver`：authorized→receipt；
- 所有分位数按 RTH/GTH 分开，不混算。

## 5. 告警边界

- 进程 active ≠ 数据可用：freshness 与 latency 分开告警；
- 数据面不健康时，先报数据面，不报“策略没信号”；
- 投递 uncertain 单独告警，不能当作成功；
- 告警消息走 `ops` lane，与策略消息分开（见
  [通知消息契约](notification-message-contract.md)）。

## 6. 验收

- 任一决策能按 correlation_id 找到 quote、decision、notification、receipt；
- 连续 5 分钟 decision P95 超过预算时产生 ops 告警；
- GTH gap 期间 freshness 告警优先于策略告警；
- 不新增常驻服务、不改变现有 owner。
