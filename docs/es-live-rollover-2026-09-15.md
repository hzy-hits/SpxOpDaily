# ES live rollover recovery — 2026-09-15

Scope: existing S3 provider input responsibility; minimal production fault repair. Baseline e610c3e9. No new data lane, process or subscription capacity.

IBKR stream has been running since September 5. Its CLI resolves IbkrSettings once before starting StreamRuntime. September 15's default resolves December, but the running collector still subscribes September. A network reconnect alone reuses that old settings object. The prior fix correctly allowed different provider maturities without rejecting known selected ES, but did not change the upstream subscription.

SessionOps now refreshes only ES/MES expiry using the existing automatic month function and existing environment precedence. Explicit IBKR_ES_EXPIRY / IBKR_MES_EXPIRY remain authoritative. StreamRuntime calls it before opening a connection and during the established CONTINUE branch, after existing conflict/policy/reconnect classification. On change it marks health unavailable and returns through existing teardown before re-opening and subscribing. No Gateway restart is requested for rollover. Existing contract-aware basis invalidation and fresh flush/exact-leg readiness still apply; a new ES contract must not inherit an old contract's basis or bar history.

Tests cover an already running September session rolling to December without failure backoff, explicit pins remaining unchanged, and existing conflict/subscription/reconnect behavior. Targeted suite: 120 passed. Release checks and live verification are reported in the delivery response. A one-time collector restart loads this fix; future automatic rolls no longer require process restart. If an explicit expiry is pinned, the code intentionally does not override it.

Complexity: two existing production files modified, +26 lines; zero production files/dependencies/config keys/services/timers/databases/tables added or deleted. Retired behavior: frozen startup-only automatic expiry. Bark and trading policies unchanged.
