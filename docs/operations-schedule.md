# Operations Schedule

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](README.md)（目录核对：2026-10-05）。

核对日期：2026-10-05。实际执行入口以已安装 unit、`systemctl list-timers` 和
`src/spx_spark/infrastructure/jobs.py` 为准。下面区分 user units、system units
与 Huey，不能把旧 24h loop 的任务表当成当前调度。

## 常驻 owner

| Owner | systemd 范围 | 当前职责 |
| --- | --- | --- |
| `spx-core.service` | user | SPX/ES 采样、特征与最终策略、独立 shock runner、必要实时任务 |
| `spx-worker.service` | user | Huey 通知任务、宏观刷新、维护、链路故障监测及启用的慢任务 |
| `spx-spark-ibkr-stream.service` | user | GTH/备用 broker 连接、配额、hot/rotation 与已授权 exact-leg 订阅 |
| `spx-spark-schwab-marketdata.service` | user | RTH 主链与 ES/上下文采集 |
| `spx-spark-schwab-oauth.service` | user | 既有 OAuth callback 与本机行情入口 |
| `ibc-gateway.service` | user | IB Gateway；API 仅 loopback，不因普通缺字段反复重启 |
| `spx-rust-core-shadow.service`、`spx-rust-normalized-bridge.service` | system | 既有 typed ingress、frames、projection 与桥接 |
| `spx-rust-report.service`、`spx-rust-delivery.service` | system | 桌图编排、Rust ledger 与每目标投递 |

`shadow` 是保留的部署名称，不能据此断言当前 report/delivery 未投入使用。
旧 `spx-spark-24h`、独立 hot-worker 和旧 notification-delivery owner 已收敛；
不要重新 enable 旧 unit。图片容器及可选 VNC 有独立用途，见专题部署说明。

## 定时工作

| 任务 | 时钟 / 计划 | 边界 |
| --- | --- | --- |
| Desk Map 源快照 | `spx-spark-order-map-status.timer`，合法 GTH/RTH 每 15 分钟 | `16:00 ET` 为收盘快照；源快照保存不等于推送 |
| 常规 Desk Map | Rust report，合法时段 `:00/:30` ET | Python 只准备投影，受 owner fence 约束 |
| 数据恢复更新 | Core 新鲜决策识别缺失能力恢复 | 不等下一半小时；重新核对当前时间，按投影去重；无恢复不伪造更新 |
| 宏观日历 | Huey 每 5 分钟检查 TTL | Worker 联网刷新，Core 只读；保留来源覆盖与 last-good |
| 桌图链路监测 | Huey 每分钟，priority 20 | 直接飞书故障/恢复告警，不能掩盖 Worker/主机整体失效 |
| 存储压力检查 | Huey 每小时 UTC `:20` | 替代退役的 storage-pressure timer，复用 session finalizer |
| 维护审计 | Huey 每日 `23:30 UTC`；user weekly timer 周日 `13:00 Asia/Shanghai` | 默认审计，不因定时到达获得任意删除权限 |
| 原始数据压缩 | user timer 每小时 `:08`，随机延迟最多 120 秒 | copy-only，保存 manifest；不能自动删除未验证原始数据 |
| 周末补压缩 | 周六/日 `08:30 Asia/Shanghai`，随机延迟最多 300 秒 | 同样复用原压缩合同 |
| Session finalizer | 每日 `18:00 America/New_York` | 自动选择完成的交易日；修复重跑幂等 |
| RTH daily acceptance | 工作日 `19:00 America/New_York` | 行情/决策/投递分开验收 |
| 每周 backtest job | 主机本地周一 `09:17`，随机延迟最多 300 秒 | 名称/启用状态不证明研究已跑完或策略有 edge |
| Rust frame retention | system timer 每日 `00:15 UTC` | 只处理已完成 UTC 日，保护当前日与写入 reserve |

Huey 使用 UTC。交易时钟由 `America/New_York` 与交易日历解释，不能全年固定
减 12 小时。所有 timer 是否已启用、漏跑补跑及实际耗时都要现场核对。
宏观、Hyperliquid、Greek shadow、IV surface 和 growth-dislocation 的开关/任务
由现有配置与 `jobs.py` 决定，不从本文复制新的 cron。

## 健康检查与数据缺口

```bash
systemctl --user list-units --type=service --state=running 'spx*' 'ibc*'
systemctl --user list-timers --all 'spx*'
systemctl list-units --type=service --state=running 'spx*'
systemctl list-timers --all 'spx-rust*'
```

按顺序核对：进程、provider/token、源时间、BBO、Greeks/OI 独立时间、
`strategy_decision.decision_at`、桌图 projection、桥接接受时间、报告保存、回执。
当前期权没有 Delta 时，不能把不断变化的 BBO 或 collector 心跳写成选腿恢复。

Core 的特征调度通常每 5 秒一轮；周期耗时应从日志测量。2026-10-05 验收时
约 1.3–2.0 秒的构建耗时不等于每 2 秒调度，也不是全天延迟保证。
历史准备共用现有有界后台线程，仍可能与实时工作争用进程资源。

## 数据与容量

IBKR 100 是全部 concurrent ticker lines。常规 RTH validation 目标为
44 hot + 20 rotation；GTH/fallback 目标为 46 hot + 38 rotation，上限 84 option
lines，实际分配还扣除 base、temporary 与 reserve。这不是全链覆盖承诺。

回放使用原始 broker 数据，按当时可用时间重建，不用推送记录挑样本。
Python warning/critical 为低于 10 GiB，Rust 不接受写后低于 10 GiB 的 append；
28 GiB action threshold 只用于提前维护。具体清理权与证据见
[存储说明](storage-plan.md)和[运行配置](runtime-configuration.md)。
