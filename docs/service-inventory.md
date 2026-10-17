# 服务与定时任务清单

<!-- documentation-status: 2026-10-18 -->
> **文档定位：现行运行与参考。** 以已安装 unit 和 `systemctl list-timers` 为准；
> 仓库里的 unit 文件不等于已经 enable。
> [全仓文档目录](README.md)（目录核对：2026-10-18）。

本文件回答一个具体问题：仓库里几十个 unit，哪些是当前 owner，哪些是可选，
哪些已经退役。它不替代现场 `systemctl` 检查，只给出仓库侧的事实与归属。

## 状态定义

| 状态 | 含义 |
| --- | --- |
| `current` | 当前 owner；由安装脚本或部署接收器启用 |
| `optional` | 存在且可用，但默认关闭或按需启用 |
| `legacy` | 保留用于手工兼容或历史回放；定时器默认禁用 |
| `external` | 不归应用仓库管理（IB Gateway、Prefect、NAS） |

## 当前应用服务（5 Python + 4 Rust）

| Owner | unit | 启动命令 | 状态 |
| --- | --- | --- | --- |
| Python Core | `spx-core.service` | `spx core run` | `current`（owner cutover 时 `--now` 启用） |
| Python Worker | `spx-worker.service` | `huey_consumer spx_spark.infrastructure.jobs.huey -w 1 -k thread` | `current` |
| IBKR 采集 | `spx-spark-ibkr-stream.service` | `spx ibkr stream` | `current` |
| Schwab 行情 | `spx-spark-schwab-marketdata.service` | `spx schwab marketdata` | `current` |
| Schwab OAuth/网关 | `spx-spark-schwab-oauth.service` | `spx schwab oauth serve` | `current` |
| Rust bridge | `spx-rust-normalized-bridge.service` | `spx-bridge run` | `current`（冻结） |
| Rust core | `spx-rust-core-shadow.service` | `spx-core serve` | `current`（冻结，`shadow` 只是保留名） |
| Rust report | `spx-rust-report.service` | `spx-report run --allow-network` | `current`（冻结） |
| Rust delivery | `spx-rust-delivery.service` | `spx-delivery run --allow-network` | `current`（冻结） |

## Python 定时任务

| unit / 任务 | 计划 | 状态 | 说明 |
| --- | --- | --- | --- |
| `spx-spark-order-map-status.timer` | 合法 GTH/RTH 每 15 分钟；`16:00 ET` 收盘快照 | `current` | `spx job order-map --status`，只写源快照 |
| `spx-spark-morning-map.timer` | 周一至五 `08:35`、`09:15 ET` | `current` | 盘前地图推送 |
| `spx-spark-order-map.timer` | 周一至五 `14:00`、`16:30 Asia/Shanghai` | `current` | `spx job order-map`；Rust owner 开启时只准备投影、不外发 |
| `spx-spark-session-finalize.timer` | 每日 `18:00 ET` | `current` | 收盘后的确定性 finalizer |
| `spx-spark-rth-daily-acceptance.timer` | 工作日 `19:00 ET` | `current` | 行情/决策/投递分开验收 |
| `spx-spark-maintenance-weekly.timer` | 周日 `13:00 Asia/Shanghai` | `current` | 默认只审计，跨阈值才 prune |
| `spx-spark-data-compact.timer` | 每小时 `:08`，随机延迟 ≤120s | `current` | copy-only，验证 manifest |
| `spx-spark-data-compact-weekend.timer` | 周六/日 `08:30 Asia/Shanghai`，延迟 ≤300s | `current` | 大 batch |
| `spx-ibkr-verifier.timer` | 每 15 分钟 | `optional` | 只读 verifier；由主机部署单独启用 |
| `spx-worker` 内 Huey 任务 | 宏观日历每 5 分钟、桌图监测每分钟、机会台账每分钟、存储压力每小时 `:20`、维护审计 `23:30 UTC` | `current` | 不是独立 unit，见 `infrastructure/jobs.py` |
| `spx-spark-post-close-review.timer` | 工作日 `17:15 ET` | `legacy`（安装脚本禁用） | 由 session finalizer 取代，保留手工入口 |
| `spx-spark-backtest-weekly.timer` | 周一 `09:17`，延迟 ≤300s | `legacy`（安装脚本禁用） | 已移到家庭研究节点 |
| `ibc-watchdog.timer` | 每 2 分钟 | `external` | 只守护 IB Gateway |

## Rust 定时任务

| unit | 计划 | 状态 |
| --- | --- | --- |
| `spx-rust-frame-retention.timer` | 每日 `00:15 UTC` | `current`（冻结） |

## 不在仓库管理

| 组件 | 归属 |
| --- | --- |
| `ibc-gateway.service`、`ibgateway-xvfb.service` | 主机侧 IB Gateway / IBC |
| Prefect + PostgreSQL | 树莓派家庭控制面 |
| NAS 归档与校验 | 家庭 NAS |
| Oracle 上的部署接收器 | `scripts/deploy-release.py`（受限制 SSH） |

## 维护规则

1. 新增 unit 前先确认不能复用现有 owner；旧 unit 退役时同时删除定时器。
2. `systemctl` 显示 `active` 只证明进程存在，不证明数据新鲜、策略可交易或消息送达。
3. 本文件的表格与 `systemd/` 不一致时，以 `systemd/` 和安装脚本为准，并修文档。
