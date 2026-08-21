> **状态（2026-08-08）：本表保留历史迁移目标用于审计。P5-2 剩余项、P6 和
> P7 均不在当前施工队列；标记为 P6/P7 不表示应立即迁移或删除。**

| 实体（unit/script/db/json） | 类型 | 当前 owner 模块 | 唯一消费者 | 目标归属（按 1.3/1.4） | 目标删除 Phase |
|---|---|---|---|---|---|
| `systemd/ibc-gateway.service` | unit | `scripts/start-ibc-gateway.sh` | user systemd | 外部 `ibc-gateway` | 保留 |
| `systemd/ibc-watchdog.service` | unit | `scripts/ibc-watchdog.sh` | `ibc-watchdog.timer` | 外部 `ibc-gateway` | 保留 |
| `systemd/ibc-watchdog.timer` | unit | user systemd timer | `ibc-watchdog.service` | 外部 `ibc-gateway` | 保留 |
| `systemd/ibgateway-xvfb.service` | unit | IB Gateway binary | user systemd | 外部 `ibc-gateway` | 保留 |
| `systemd/spx-ibkr-verifier.service` | unit | direct `spx verify ibkr` | `spx-ibkr-verifier.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-ibkr-verifier.timer` | unit | user systemd timer | `spx-ibkr-verifier.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-24h.service` | unit | `scripts/run-24h-service.sh` / `service_loop.py` | user systemd | 热任务 `spx-core`、慢任务 `spx-worker` | P3-3 |
| `systemd/spx-spark-backtest-weekly.service` | unit | `scripts/backtest-0dte-levels.py` | `spx-spark-backtest-weekly.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-backtest-weekly.timer` | unit | user systemd timer | `spx-spark-backtest-weekly.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-data-compact-weekend.service` | unit | `scripts/run-data-compact.sh` | `spx-spark-data-compact-weekend.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-data-compact-weekend.timer` | unit | user systemd timer | `spx-spark-data-compact-weekend.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-data-compact.service` | unit | `scripts/run-data-compact.sh` | `spx-spark-data-compact.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-data-compact.timer` | unit | user systemd timer | `spx-spark-data-compact.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-es-bar-sampler.service` | unit | `application.runtime.es_bar_sampler` | user systemd | `spx-core` TaskGroup | P3-1 |
| `systemd/spx-spark-ibkr-stream.service` | unit | `ibkr.stream_collector` | user systemd | `spx-ibkr` | P3 |
| `systemd/spx-spark-intraday-shock-hot.service` | unit | `application.runtime.intraday_shock_hot_worker` | user systemd | `spx-core` TaskGroup | P3-2 |
| `systemd/spx-spark-maintenance-daily.service` | unit | `scripts/run-maintenance-daily.sh` | `spx-spark-maintenance-daily.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-maintenance-daily.timer` | unit | user systemd timer | `spx-spark-maintenance-daily.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-maintenance-weekly.service` | unit | `scripts/run-maintenance-weekly.sh` | `spx-spark-maintenance-weekly.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-maintenance-weekly.timer` | unit | user systemd timer | `spx-spark-maintenance-weekly.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-market-features-hot.service` | unit | `application.runtime.market_features_hot_worker` | user systemd | `spx-core` TaskGroup | P3-2 |
| `systemd/spx-spark-market-regime-signal.service` | unit | `application.runtime.market_regime_signal` | user systemd | `spx-core` TaskGroup | P3-2 |
| `systemd/spx-spark-morning-map.service` | unit | `morning_map` | `spx-spark-morning-map.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-morning-map.timer` | unit | user systemd timer | `spx-spark-morning-map.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-notification-delivery.service` | unit | `notifier.delivery_worker` | user systemd | `spx-worker` Huey consumer | P4-2 |
| `systemd/spx-spark-order-map-status.service` | unit | direct `spx job order-map --status` | `spx-spark-order-map-status.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-order-map-status.timer` | unit | user systemd timer | `spx-spark-order-map-status.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-order-map.service` | unit | `order_map` | `spx-spark-order-map.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-order-map.timer` | unit | user systemd timer | `spx-spark-order-map.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-post-close-review.service` | unit | `post_close_review` | `spx-spark-post-close-review.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-post-close-review.timer` | unit | user systemd timer | `spx-spark-post-close-review.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-rth-daily-acceptance.service` | unit | `application.order_map.rth_daily_acceptance` | `spx-spark-rth-daily-acceptance.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-rth-daily-acceptance.timer` | unit | user systemd timer | `spx-spark-rth-daily-acceptance.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-schwab-marketdata.service` | unit | `schwab.collector:loop_main` | user systemd | `spx-schwab` | P3 |
| `systemd/spx-spark-schwab-oauth.service` | unit | `schwab.oauth_service` | user systemd | `spx-schwab` | P3 |
| `systemd/spx-spark-schwab-reauth-reminder.service` | unit | `scripts/run-schwab-reauth-reminder.sh` | `spx-spark-schwab-reauth-reminder.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-schwab-reauth-reminder.timer` | unit | user systemd timer | `spx-spark-schwab-reauth-reminder.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-session-finalize.service` | unit | `session_finalize` | `spx-spark-session-finalize.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-session-finalize.timer` | unit | user systemd timer | `spx-spark-session-finalize.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-spx-minute-sampler.service` | unit | `application.runtime.spx_minute_sampler` | user systemd | `spx-core` TaskGroup | P3-1 |
| `systemd/spx-spark-storage-pressure.service` | unit | `session_finalize --pressure-check` | `spx-spark-storage-pressure.timer` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-storage-pressure.timer` | unit | user systemd timer | `spx-spark-storage-pressure.service` | `spx-worker` periodic task | P4 |
| `systemd/spx-spark-surface-dashboard.service` | retired unit | `surface_dashboard` | none | 用户于 2026-08-21 明确退役 | 已删除 |
| `systemd/spx-spark-surface-live.service` | retired unit | `surface_live_session_http` | none | 用户于 2026-08-21 明确退役 | 已删除 |
| `systemd/spx-spark-surface-replay-warm.service` | retired unit | replay catalog warmer | none | 用户于 2026-08-21 明确退役 | 已删除 |
| `systemd/spx-spark-surface-replay-warm.timer` | retired unit | user systemd timer | none | 用户于 2026-08-21 明确退役 | 已删除 |
| `systemd/spx-spark-surface-replay.service` | retired unit | `surface_replay_service` | none | 用户于 2026-08-21 明确退役 | 已删除 |
| `rust/systemd/spx-core.service.example` | unit template | `spx-core` | deployment tooling | 无；Rust 控制面退役 | P6 |
| `rust/systemd/spx-delivery.service.example` | unit template | `spx-delivery` | deployment tooling | 无；Rust 控制面退役 | P6 |
| `rust/systemd/spx-report.service.example` | unit template | `spx-report` | deployment tooling | 无；Rust 控制面退役 | P6 |
| `rust/systemd/spx-rust-core-shadow.service` | unit | `spx-core` | host systemd | Python `spx-core` / `spx.sqlite` | P6 |
| `rust/systemd/spx-rust-delivery.service` | unit | `spx-delivery` | host systemd | `spx-worker` | P6 |
| `rust/systemd/spx-rust-frame-retention.service` | unit | `spx-core archive/prune-frames` | `spx-rust-frame-retention.timer` | `spx-worker` retention task | P6 |
| `rust/systemd/spx-rust-frame-retention.timer` | unit | host systemd timer | `spx-rust-frame-retention.service` | `spx-worker` retention task | P6 |
| `rust/systemd/spx-rust-normalized-bridge.service` | unit | `spx-bridge` | host systemd | direct Python ownership | P6 |
| `rust/systemd/spx-rust-report.service` | unit | `spx-report` | host systemd | `spx-worker` | P6 |
| `spx-spark-alert-profile` | retired script | `alert_profile:main` | direct `spx ops alert-profile` | `spx` operator command | P5 |
| `spx-spark-alert-engine` | script | `alert_engine:main` | service-loop registry | direct `spx-core` call | P3-3 |
| `spx-spark-provider-failover` | script | `provider_failover_controller:main` | service-loop registry | direct `spx-core` call | P3-3 |
| `spx-spark-data-platform` | script | `data_platform.cli:main` | human/operator tooling | `spx data` command | P5 |
| `spx-spark-data-compact` | script | `data_platform.lake.compact:main` | compact wrapper/units | `spx-worker` job and `spx data compact` | P5 |
| `spx-spark-intraday-shock` | script | `intraday_shock:main` | hot worker/service-loop | direct `spx-core` call | P3-2 |
| `spx-spark-greek-shadow` | script | `greek_shadow:main` | service-loop registry | direct `spx-core` call | P3-3 |
| `spx-spark-ibkr-verifier` | script | `ibkr.verifier:main` | verifier wrapper/unit | `spx-worker` job / `spx status` | P5 |
| `spx-spark-ibkr-collector` | script | `ibkr.collector:main` | collector wrapper/service-loop | `spx-ibkr` direct call | P3 |
| `spx-spark-ibkr-stream` | retired script | `ibkr.stream_collector:main` | direct `spx ibkr stream` | `spx-ibkr` service | P5 |
| `spx-spark-ibkr-farm-probe` | script | `ibkr.farm_health:main` | `ibc-watchdog.sh` | external gateway watchdog | P5 |
| `spx-spark-ibkr-positions` | script | `ibkr.position_watcher:main` | position wrapper/service-loop | direct `spx-ibkr` call | P3 |
| `spx-spark-ibkr-trading-hours-report` | script | `ibkr.trading_hours_report:main` | operator wrapper | `spx report` command | P5 |
| `spx-spark-iv-surface` | script | `iv_surface:main` | surface wrapper/service-loop | direct `spx-core` call | P3-3 |
| `spx-spark-hyperliquid-collector` | script | `hyperliquid.collector:main` | collector wrapper/service-loop | direct `spx-core` call | P3-3 |
| `spx-spark-latest-state` | script | `latest_state:main` | operator wrapper | `spx status` command | P5 |
| `spx-spark-maintenance` | script | `maintenance:main` | maintenance wrappers/units | `spx-worker` job / `spx ops` | P5 |
| `spx-spark-micopedia-guidance` | script | `strategy.micopedia:main` | operator wrapper | `spx report` command | P5 |
| `spx-spark-steven` | script | `strategy.steven:main` | service-loop/strategy replay | direct strategy call | P3-3 |
| `spx-spark-steven-replay` | script | `strategy.steven_replay:main` | strategy replay tooling | `spx replay` command | P5 |
| `spx-spark-morning-map` | script | `morning_map:main` | morning-map wrapper/unit | `spx-worker` job | P4 |
| `spx-spark-order-map` | script | `order_map:run` | order-map wrapper/units | `spx-worker` job | P4 |
| `spx-spark-mock-collector` | script | `mock_collector:main` | operator/test wrapper | `spx ops` command | P5 |
| `spx-spark-options-map` | script | `options_map:main` | operator wrapper | `spx report` command | P5 |
| `spx-spark-post-close-review` | script | `post_close_review:main` | review wrapper/unit | `spx-worker` job | P4 |
| `spx-spark-rth-daily-acceptance` | script | `application.order_map.rth_daily_acceptance:main` | acceptance unit | `spx-worker` job | P4 |
| `spx-spark-runtime-mode` | script | `runtime_mode:main` | operator tooling | `spx ops` command | P5 |
| `spx-spark-service-loop` | script | `service_loop:main` | `run-24h-service.sh` | direct Core/Worker calls | P3-3 |
| `spx-spark-es-bar-sampler` | script | `application.runtime.es_bar_sampler:main` | sampler wrapper/unit | `spx-core` TaskGroup | P3-1 |
| `spx-spark-spx-minute-sampler` | script | `application.runtime.spx_minute_sampler:main` | sampler wrapper/unit | `spx-core` TaskGroup | P3-1 |
| `spx-spark-market-features-hot-worker` | script | `application.runtime.market_features_hot_worker:main` | hot-worker wrapper/unit | `spx-core` TaskGroup | P3-2 |
| `spx-spark-market-regime-signal` | script | `application.runtime.market_regime_signal:main` | regime unit | `spx-core` TaskGroup | P3-2 |
| `spx-spark-intraday-shock-hot-worker` | script | `application.runtime.intraday_shock_hot_worker:main` | hot-worker wrapper/unit | `spx-core` TaskGroup | P3-2 |
| `spx-spark-sampling-plan` | script | `sampling:main` | operator wrapper | `spx status` command | P5 |
| `spx-spark-schwab-verifier` | script | `schwab.verifier:main` | verifier wrapper | `spx status` command | P5 |
| `spx-spark-schwab-collector` | script | `schwab.collector:main` | service-loop registry | `spx-schwab` direct call | P3 |
| `spx-spark-schwab-marketdata` | script | `schwab.collector:loop_main` | marketdata wrapper/unit | `spx-schwab` service | P5 |
| `spx-spark-schwab-oauth` | script | `schwab.oauth_service:main` | OAuth wrapper/unit | `spx-schwab` service | P5 |
| `spx-spark-surface-dashboard` | script | `surface_dashboard:main` | dashboard wrapper/unit | `spx-core` FastAPI | P3-1 |
| `spx-spark-surface-dashboard-replay` | script | `surface_dashboard_replay:main` | human/operator tooling | `spx replay` command | P5 |
| `spx-spark-surface-replay-service` | script | `surface_replay_service:main` | replay wrapper/unit | `spx-core` FastAPI | P3-1 |
| `spx-spark-surface-live-service` | script | `surface_live_session_http:main` | live wrapper/unit | `spx-core` FastAPI | P3-1 |
| `/srv/data/spx-spark/data/ledger/notification_delivery_outbox.sqlite` | db | `notifier.delivery_outbox` | `notifier.delivery_worker` | `spx.sqlite.notification_events/attempts` | P4-2 |
| `/srv/data/spx-spark/data/ledger/notification_delivery.sqlite` | db | `notifier.receipts` | notification reporting | `spx.sqlite.notification_attempts` | P4-2 |
| `/srv/data/spx-spark/data/ledger/domain_event_outbox.sqlite` | db | `infrastructure.ledger.outbox` | realtime application | `spx.sqlite` domain tables | P5-1 |
| `/srv/data/spx-spark/data/runtime/research-ledger.sqlite3` | db | `data_platform.adapters.sqlite_ledger` | research catalog/telemetry | `spx.sqlite.decisions` | P5-1 |
| `/var/lib/spx-spark-core-shadow/ledger/operations.sqlite` | db | Rust `spx-ledger` | Rust core/report/delivery | `spx.sqlite` | P6 |
| `/srv/data/spx-spark/data/latest/state.json` | json | provider storage writer | Python market readers | provider last-known snapshot | 保留 |
| `/srv/data/spx-spark/data/latest/ibkr_stream_health.json` | json | `ibkr.stream_collector` | readiness/reporting | `spx-ibkr` provider snapshot | 保留 |
| `/srv/data/spx-spark/data/latest/schwab_collector_state.json`、`schwab_stream_shadow.json` | json | `schwab.collector` | readiness/reporting | `spx-schwab` provider snapshot | 保留 |
| `/srv/data/spx-spark/data/latest/es_bars_5m.json`、`spx_bars_1m.json`、`spx_bars_5m.json` | json | sampler/bar builder | market features/order map | `spx-core` memory + read-only projection | P3 |
| `/srv/data/spx-spark/data/latest/market_feature_state.json`、`minute_market_frame.json`、`option_structure_frame.json` | json | `application.market_features` | order map/reports | `spx-core` direct calls + export | P3 |
| `/srv/data/spx-spark/data/latest/order_map_state.json`、`decision_context.json`、`desk_map_projection.json` | json | `application.order_map` | reports/notifications/Rust bridge | `spx-worker` artifact then `spx.sqlite` | P5 |
| `/srv/data/spx-spark/data/latest/trade_intent*.json`、`trade_candidate_state.json` | json | trade-intent runtime | notification/reporting | `spx.sqlite.decisions/decision_legs` | P5 |
| `/srv/data/spx-spark/data/latest/gth_*candidate*.json`、`gth_dip_reclaim_signal.json`、`gth_path_ranks.json` | json | GTH market-feature modules | order map/notifications/replay | unified `strategy_decision` artifact | S5/P5 |
| `/srv/data/spx-spark/data/latest/gamma_*.json`、`exposure_map.json`、`iv_surface.json`、`spxw_0dte_greeks_reference.json` | json | analytics/features | strategy/order map/dashboard | `spx-core` direct calls + export | P3/P7 |
| `/srv/data/spx-spark/data/latest/experimental_research_signals*.json`、`strategy_distribution_forecast.json` | json | research projection | order map/reporting | immutable research export | 保留 |
| `/srv/data/spx-spark/data/latest/*_lease.json` | json | hot worker/samplers | duplicate-writer guards | structured concurrency ownership | P3 |
| `/srv/data/spx-spark/data/latest/*_state.json`（非 provider、非 control） | json | feature-specific writers | order map/report/replay | Core memory or `spx.sqlite` | P3/P5 |
| `/srv/data/spx-spark/data/latest/alert_review_audit.jsonl`、`level_trigger_pricing_outcomes.json` | json/jsonl | alert/order-map audit | replay/review | immutable historical artifact | 保留 |
| `/var/lib/spx-spark-bridge-shadow/health.json`、`state.json` | json | Rust `spx-bridge` | operations/verification | Python health/provider ownership | P6 |
| `/var/lib/spx-spark-core-shadow/latest/*.json` | json | Rust `spx-core` | Rust report/delivery | Python projections / `spx.sqlite` | P6 |
| `/var/lib/spx-spark-report-shadow/health.json` | json | Rust `spx-report` | operations/verification | `spx-worker` health | P6 |
