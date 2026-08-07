# Change Brief — P4-1 `spx-worker`

## Goal

- 用一个单线程 Huey worker 接管三个低风险周期任务，减少独立 timer/service 数量。
- 建立唯一的 `spx.sqlite` Alembic 基线，为后续通知 outbox 收敛做准备。

## Current owner

- `spx-spark-maintenance-daily.*` → `scripts/run-maintenance-daily.sh`。
- `spx-spark-storage-pressure.*` → `scripts/run-session-finalize.sh --pressure-check`。
- `spx-spark-schwab-reauth-reminder.*` → `application.schwab_reauth_reminder.run`。

## Reuse

- 原样调用 `maintenance.run`、`session_finalize.run`、`schwab_reauth_reminder.run`。
- 使用 Huey `SqliteHuey`/`crontab`、SQLAlchemy/Alembic；不复制任务业务逻辑。

## Replace/Delete

- 周末切换并验证后删除上述三对 timer/service 及只供它们使用的 shell wrapper。
- 本卡不删除 `spx-spark-24h`，其余任务尚未完成归属迁移。

## Dependency decision

- 新增 blessed dependencies：Huey、SQLAlchemy、Alembic。
- 它们分别替代 timer 扇出和手写 schema migration；不再新增自研 scheduler 或 DDL。

## Boundaries

- 进程：只新增 `spx-worker.service`，单 worker、单线程、单 periodic scheduler。
- DB：新增 `/srv/data/spx-spark/huey.sqlite` 与 `spx.sqlite`；初始业务表只有
  `notification_events`、`notification_attempts`。
- 文件：继续复用三个任务现有输出路径。
- 通知：仅 Schwab 周提醒沿用现有人工通知；不迁移实时 outbox/delivery owner。
- 自动交易权限：无；不读取账户、不下单。

## Schedule mapping

Huey 固定使用 UTC；上海时区没有 DST。

| 旧 `OnCalendar` | Huey UTC `crontab` |
|---|---|
| 每日 07:30 Asia/Shanghai | `minute=30, hour=23` |
| 每小时 :20 | `minute=20` |
| 周日 20:00 Asia/Shanghai | `minute=0, hour=12, day_of_week=0` |

旧 timer 的 randomized delay 不进入新调度；Huey 每分钟只检查一次，单线程保证任务不并发。

## Minimal path

`crontab → Huey SQLite queue → 现有 run() → 现有 artifact/notification output`。

## Tests

- 三个 task 只调用对应入口，并把非零退出码变成 Huey 失败。
- Alembic upgrade 只创建两张业务表；非法 notification status 被 SQLite CHECK 拒绝。
- 单 worker unit、UTC schedule、数据库路径均为显式值。

## Non-goals

- 不迁移实时通知 outbox，不改 report/delivery owner，不清理其他 timer。
- 不引入 ORM entity、repository、第二个 queue 或第二套 migration。
