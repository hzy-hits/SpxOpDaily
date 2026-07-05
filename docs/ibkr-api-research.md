# IBKR API Research Notes

Date: 2026-07-04

## Goal

Use IBKR market-data subscriptions without disrupting manual phone or desktop trading.

Target behavior:

- 24-hour IBKR eligibility on trading days when Gateway is authenticated and no competing session blocks data
- weekend auto mode pauses IBKR for cleanup and audit
- agent-triggered temporary monitoring outside the normal window
- no automatic session fighting
- no automatic order placement

## API Surfaces

### TWS API / IB Gateway

This is the best IBKR path for live streaming market data.

Facts from IBKR docs:

- API socket connections must be enabled in TWS or IB Gateway.
- TWS default ports are 7496 live and 7497 paper.
- IB Gateway default ports are 4001 live and 4002 paper.
- Read-only API is enabled by default and blocks API orders.
- Live and historical data through the API require live data permissions and subscriptions.

Decision:

- Use TWS API through IB Gateway for market-data collection.
- Keep Read-Only API enabled for MVP.
- Keep API socket bound to localhost.
- Do not expose 4001 or 4002 publicly.

### Client Portal / Web API

This does not remove the session problem.

Useful facts:

- Client Portal API has authentication/session status endpoints.
- `/iserver/auth/status` reports fields such as `authenticated`, `connected`, and `competing`.
- `/iserver/auth/ssodh/init` has a `compete` parameter that determines whether other brokerage sessions should be disconnected to prioritize this connection.
- OAuth direct access to `https://api.ibkr.com` exists, but IBKR describes approval/onboarding for licensed financial advisors, organizations, IBrokers, first-party institutional users, and third-party services.

Decision:

- Do not use Client Portal API as the primary market-data path for MVP.
- It can be useful later for session state probes or account metadata.
- Never set `compete=true` by default.
- Do not use Client Portal `compete=true` for background monitoring.

## Session Policy

Default posture:

- IBKR is opportunistic.
- Schwab and chain/prediction feeds stay always-on.
- If IBKR is unavailable, the system keeps running.
- If IBKR detects a competing session, mark IBKR unavailable and do not fight for the session.
- While a competing phone/desktop session appears active, probe IBKR availability every `IBKR_CONFLICT_PROBE_SECONDS`.
- A probe is not a takeover. It must not request a competing brokerage session or force-disconnect another device.

Agent override modes:

- `auto`: use configured eligibility policy; default is 24 hours on trading days and paused on weekends
- `ibkr_on`: allow IBKR now, with TTL
- `protected`: block IBKR now, with TTL
- `ibkr_off`: block IBKR now, with TTL

The override is intentionally a local JSON file, not a permanent config change.

Command examples:

```bash
cd /home/ubuntu/spx-spark
uv run spx-spark-runtime-mode status
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual monitor request"
uv run spx-spark-runtime-mode protected --ttl-minutes 180 --reason "phone trading"
uv run spx-spark-runtime-mode auto
uv run spx-spark-runtime-mode clear
```

## Beijing 01:05-08:00 High-Value Window

During U.S. daylight saving time:

- Beijing 01:05 = 13:05 ET
- Beijing 04:00 = 16:00 ET
- Beijing 08:00 = 20:00 ET

During U.S. standard time:

- Beijing 01:05 = 12:05 ET
- Beijing 05:00 = 16:00 ET
- Beijing 08:00 = 19:00 ET

This window captures:

- U.S. afternoon
- SPX/SPXW close behavior
- late-day vol regime
- after-hours ETF/futures context
- post-close report and data-quality checks

It does not provide a complete full-day IBKR/OPRA tick history.

The default policy can still allow IBKR 24 hours a day on trading days. This window is highlighted because it is the highest-value capture period after the user's usual manual trading period, not because IBKR must be disabled outside it.

On weekends, auto mode should pause IBKR and use the time for maintenance. Use `ibkr_on` only for a deliberate weekend or holiday data check.

## Market Data Limits

Useful IBKR facts:

- Level 1 market data provides live watchlist data, tick-by-tick data, historical bars, and historical ticks.
- OPRA provides U.S. options L1 data.
- Options Greeks also require relevant underlying data.
- Indices can require separate subscriptions from derivatives.
- Initial accounts have 100 concurrent real-time market-data lines.
- Tick-by-tick request capacity is a fraction of total market-data lines.
- Market data is provisioned per user.
- Regulatory snapshots are not available for ETFs, options, futures, or instruments other than common U.S. stocks.

Implication:

- Do not assume 150-300 option contracts can stream simultaneously through IBKR.
- Do not buy snapshot data expecting it to solve SPXW option streaming.
- Subscribe only the near-ATM SPXW window first.
- Use periodic option-chain refresh for broad context.
- Stream only selected strikes and underliers.

Recommended IBKR line tiers:

- Tier 0: 20-40 option contracts, ATM +/- 25 to 50 points, 0DTE only
- Tier 1: 40-80 option contracts, ATM +/- 50 to 100 points, 0DTE plus selected 1DTE
- Tier 2: 80-120 option contracts, wider selected strikes if account lines permit
- Above 120: only after verifying available line allocation and pacing behavior

How to check available IBKR lines:

- In TWS, use the Market Data Lines monitor. IBKR docs mention the `Ctrl-Alt-=` shortcut.
- Compare TWS watchlist line usage plus API line usage.
- Run the project verifier with a small `IBKR_MAX_OPTION_LINES` first.
- Increase in steps and watch for line-cap or market-data errors.

Relevant IBKR errors:

- `100`: max number of tickers reached
- `354`: not subscribed to requested market data
- `10090`: part of requested market data is not subscribed
- `10186`: requested market data is not subscribed and delayed market data is not enabled
- `10197`: no market data during competing session

## Implementation Rules

Collectors should:

- check runtime mode before connecting IBKR
- attempt IBKR connection only when allowed
- keep Schwab/Hyperliquid/Polymarket running regardless of IBKR state
- tag every feature with provider and quality
- treat IBKR errors 1100, 1101, 1102, 10197, and 10090 as data-quality events
- avoid reconnect storms
- never place orders in MVP

IBKR connection behavior:

- ordinary connection failure: retry inside the allowed window after `IBKR_CONNECT_RETRY_SECONDS`
- competing session: mark unavailable; keep Schwab alerts active; probe every `IBKR_CONFLICT_PROBE_SECONDS`
- agent `ibkr_on`: allow connection outside the window, still with no session fighting
- agent `protected`: block IBKR even inside the schedule

Expected alert behavior:

- phone/desktop owns the broker session: IBKR fields are unavailable and alerts are generated from Schwab, Hyperliquid, Polymarket, and other non-conflicting feeds
- phone/desktop releases the session: the next IBKR probe can reconnect or resume subscriptions, then IBKR-backed alerts become available again
- if IBKR Gateway requires fresh login or 2FA: probes will keep failing until the Gateway is authenticated; the system still uses fallback feeds
- no background process should repeatedly kick the phone session
- if IBC Gateway is configured: use normal Gateway login with `ReadOnlyApi=yes` and `ExistingSessionDetectedAction=secondary`; systemd may restart IBC every 60 seconds, but IBC should yield when another session owns the account

## Sources

- TWS/Gateway API setup: https://www.interactivebrokers.com/campus/trading-lessons/installing-configuring-tws-for-the-api/
- IB Gateway launch/auth lesson: https://www.interactivebrokers.com/campus/trading-lessons/launching-and-authenticating-the-gateway/
- Client Portal API v1 docs: https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
- Market data subscriptions: https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/
- TWS API message codes: https://interactivebrokers.github.io/tws-api/message_codes.html
