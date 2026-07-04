# Data Source And Runtime Decision Memo

Date: 2026-07-04

## Decision

Use the Oracle Linux host as the always-on collector and alert host.

Do not make IBKR Gateway the primary always-on data source while manual trading is normally done from phone or desktop with the same IBKR username. Use IBKR for entitlement verification, short live sessions, and fallback checks.

Use Schwab as the first non-IBKR candidate for always-on market data:

- SPY/QQQ/IWM ETF quotes and options
- Risk-proxy ETFs
- SPX/XSP/SPXW option-chain tests
- ES/MES quote and futures-streaming tests if the account is entitled

Use Hyperliquid, TradeXYZ, and Polymarket as always-on context sources because they do not interfere with broker login sessions.

## Why

IBKR is cheap and already subscribed, but it is operationally awkward for this use case. IBKR API data goes through TWS or IB Gateway, and market-data subscriptions are user/session sensitive. IBKR documents error 10197 as a competing-session condition when live and paper accounts request live market data at the same time. IBKR also documents that API data can differ from free on-platform data because API data is considered off-platform.

Schwab is operationally cleaner for this project if the account can return the needed SPX/XSP/SPXW option data. The Schwab API still needs an approved developer app and OAuth token management, but it does not require a persistent Java GUI trading platform on the server.

Schwab should be treated as near-real-time broker data, not guaranteed exchange-direct tick data. Schwab's public streaming-data overview describes updates up to roughly once per second on Schwab.com. That is enough for a first dashboard and alert engine, but not a replacement for a paid OPRA tick feed if the goal becomes historical tick replay or market-microstructure research.

## Practical Runtime Split

### Oracle Linux Host

Run:

- Schwab verifier and collector
- Hyperliquid collector
- TradeXYZ collector
- Polymarket collector
- Feature engine
- Alert engine
- Dashboard backend
- Event-triggered Spark summaries

Avoid:

- automatic order placement
- always-on IBKR Gateway with auto-reconnect using the same username as phone trading

### Windows Desktop

Use for:

- manual trading when at desk
- thinkorswim or IBKR desktop UI
- occasional visual debugging

Risk:

- Windows updates and reboots make it a poor always-on collector.

### MacBook

Use for:

- portable manual monitoring
- one-time OAuth/token setup
- emergency access to the Oracle dashboard over SSH tunnel or browser

Risk:

- not stable as the permanent data host because it travels.

## Schwab Validation Checklist

The key question is not "does Schwab have options API". It is:

- Can this exact account return real-time SPX option chains?
- Can it distinguish SPX monthly from SPXW daily/weekly contracts?
- Can it return XSP chains?
- Can it stream level-one option quotes for selected SPX/SPXW/XSP option symbols?
- Are Greeks usable and current enough for 0DTE monitoring?
- Can it quote or stream `/ES` and `/MES` with the current account entitlement?
- Are returned quotes live, delayed, indicative, or stale?
- What is the observed update frequency during active RTH option trading?

Test symbols:

- ETF quotes: `SPY`, `QQQ`, `IWM`
- ETF options: near-ATM `SPY`, `QQQ`, `IWM`
- Index option chains: `SPX`, `XSP`
- Index quotes: `$SPX`, `$VIX`, `$VIX1D`, `$VIX9D`, `$VIX3M`, `$VVIX`, `$SKEW`
- Futures quotes: `/ES`, `/MES`

Expected useful Schwab API surfaces:

- quote endpoint for symbols including `/ES`
- option-chain endpoint with strike count, date range, entitlement, and underlying quote controls
- level-one option streaming with bid, ask, last, volume, open interest, IV, delta, gamma, theta, vega, underlying price, quote time, and trade time fields
- level-one futures streaming with bid, ask, last, volume, open interest, mark, active symbol, trading hours, quote time, and trade time fields

## Headless OAuth Note

Schwab OAuth can be generated on a machine with a browser and then reused on the Oracle host. Current community library docs describe:

- a login flow that opens a browser and writes a token file
- a manual flow for browserless/cloud environments
- a token-from-file flow for reusing an existing token
- refresh tokens that must be recreated after roughly seven days

This is still operational work, but it is less intrusive than keeping IB Gateway logged in all day.

## IBKR Role

Keep IBKR support in the project because it is still valuable:

- verifies paid OPRA, CME, and Cboe index subscriptions
- checks SPXW Greeks directly from IBKR
- checks ES/MES when the phone/desktop trading session is not active
- provides a fallback for broker-specific discrepancies

Recommended behavior:

- no auto-reconnect that fights the phone session
- pause collector when disconnected
- manual restart after phone trading is done
- keep API read-only for MVP

### IBKR Escape Hatches

IBKR data subscriptions are not wasted, but they should be used where they have the most marginal value.

Confirmed IBKR constraints:

- API market data requires a live Level 1 subscription for most securities.
- OPRA provides U.S. options L1 data, but options Greeks also depend on the underlying market data.
- Cboe Streaming Market Indexes provides L1 data for Cboe indices such as SPX.
- Index data can require a separate subscription from derivatives data.
- Market data is provisioned per IBKR user, not just per account.
- Multiple users on the same account can be used to run API and manual platforms at the same time, but market data fees are charged per subscribed user.
- Paper users can share live market data from the live user only under IBKR's sharing rules, and competing live/paper sessions can still block live market data.

Practical options:

1. Single IBKR user, no extra data cost:
   - Use IBKR only in scheduled windows.
   - Protect mobile trading sessions.
   - Fall back to Schwab and non-broker sources when IBKR is unavailable.
   - Best near-term option.

2. Second IBKR user for API:
   - Cleaner concurrent sessions.
   - Requires subscribing the second user to the needed market data.
   - Useful only if OPRA/Cboe/CME duplicate fees are acceptable.

3. Paper-sharing approach:
   - May help with testing.
   - Not reliable enough as the primary live data path because live/paper competing sessions can still interrupt data.

4. External data vendor:
   - Avoids IBKR session problems entirely.
   - Usually costs more than IBKR.
   - Becomes attractive if continuous OPRA/SPXW data is more important than keeping broker data cheap.

Therefore, treat current IBKR subscriptions as high-quality verification and scheduled capture:

- SPX/SPXW chain and Greeks during allowed windows
- Cboe vol/index checks
- ES/MES checks when available
- close-window snapshots
- daily entitlement and data-quality verification

## Time-Window Strategy

The user normally trades manually from roughly 14:00 to 01:00 Asia/Shanghai time and generally does not manually watch after roughly 03:00 Asia/Shanghai.

During U.S. daylight saving time, New York is 12 hours behind Beijing:

- U.S. regular cash session 09:30-16:00 ET = 21:30-04:00 Beijing time
- Beijing 01:00 = 13:00 ET
- Beijing 03:00 = 15:00 ET

During U.S. standard time, New York is 13 hours behind Beijing:

- U.S. regular cash session 09:30-16:00 ET = 22:30-05:00 Beijing time
- Beijing 01:00 = 12:00 ET
- Beijing 03:00 = 14:00 ET

Implication:

- IBKR cannot be the only live source without losing most of the RTH session if it only starts after 03:00 Beijing.
- Starting IBKR after 03:00 Beijing is still useful because it captures the final one to two hours, close behavior, SPXW decay, closing auction context, and post-trade verification.
- If manual phone trading truly ends around 01:00 Beijing, the 01:05-08:00 window is the highest-value IBKR capture period.

### Beijing 14:00-21:30 Pre-Open Window

During U.S. daylight saving time:

- Beijing 14:00-16:00 = 02:00-04:00 ET
- Beijing 16:00-21:30 = 04:00-09:30 ET

During U.S. standard time:

- Beijing 14:00-17:00 = 01:00-04:00 ET
- Beijing 17:00-22:30 = 04:00-09:30 ET

This window matters because liquidity is thin and news shocks can create large retracements before the U.S. cash open.

Pre-open source priority:

- ES/MES or Schwab futures quotes if available
- Hyperliquid SPX mark, funding, open interest, order-book imbalance, and large trades
- Polymarket event probability jumps
- macro/news calendar and surprise windows
- SPY/QQQ/IWM premarket quotes after 04:00 ET
- SPX/SPXW GTH quotes only if the broker/API actually supports them with acceptable spreads

Pre-open alerts should be graded differently from RTH alerts:

- `info`: regime changed, no action needed
- `watch`: possible dip/reversal setup; wait for confirmation
- `device_required`: enough context to justify opening the trading device and preparing an order
- `avoid`: liquidity/spread/news risk is too poor for manual execution

Do not let the system auto-place orders in this window. The intended workflow is:

1. Collector detects a setup.
2. Alert includes source, liquidity score, spread quality, stale flags, and invalidation level.
3. User opens phone/desktop and decides whether to place a limit order.
4. System records the alert and later compares forward return and option response.

Schwab API should be the preferred broker data source during this window if it passes the account-level verifier, because it should not require a persistent TWS/Gateway session. Still test concurrent use explicitly:

- keep Schwab API streaming quotes
- log into Schwab mobile or thinkorswim mobile
- place or stage a tiny non-risky test order workflow, then cancel
- confirm API quotes continue and the mobile session is not displaced

Until this is tested, mark Schwab concurrent behavior as `assumed_non_conflicting`, not `verified_non_conflicting`.

Recommended operating modes:

- `manual_protected`: do not connect or auto-reconnect IBKR; use Schwab, Hyperliquid, TradeXYZ, Polymarket, and any non-conflicting feeds.
- `ibkr_afternoon`: connect IBKR after manual trading is done; collect SPX/SPXW Greeks, SPX/VIX/VVIX/SKEW, ES/MES, and ETF confirmations.
- `ibkr_close`: force an IBKR verification and high-resolution snapshot into the close.
- `post_close`: stop IBKR, write session report, and release the broker session.

Default automated policy:

- allow IBKR connection attempts 24 hours a day
- in `auto` mode, pause IBKR collection on weekends for maintenance
- keep `STRICT_NO_SESSION_FIGHT=true`
- keep Schwab and chain/prediction feeds running at all times
- if IB Gateway is already authenticated, the IBKR collector may connect automatically
- if IB Gateway is not authenticated, do not solve login by storing credentials until explicitly approved
- if mobile or desktop trading kicks IBKR out, mark IBKR unavailable and do not immediately fight for the session
- while IBKR is unavailable because of a competing session, keep sending alerts from Schwab and non-broker feeds
- probe IBKR availability every 5 minutes by default (`IBKR_CONFLICT_PROBE_SECONDS=300`)
- a probe is not a takeover and must not force-disconnect the phone or desktop session
- retry ordinary connection failures every few minutes
- do not retry competing-session conflicts unless `STRICT_NO_SESSION_FIGHT=false`

Agent override policy:

- `auto`: use the configured eligibility policy
- `ibkr_on`: temporarily allow IBKR monitoring outside the normal eligibility policy
- `protected`: temporarily block IBKR, even when it would normally be eligible
- `ibkr_off`: temporarily block IBKR
- all overrides should have a TTL unless deliberately cleared
- `ibkr_on` can override weekend maintenance for a deliberate manual check

Command examples:

```bash
cd /home/ubuntu/spx-spark
uv run spx-spark-runtime-mode status
uv run spx-spark-runtime-mode ibkr-on --ttl-minutes 120 --reason "manual monitor request"
uv run spx-spark-runtime-mode protected --ttl-minutes 180 --reason "phone trading"
uv run spx-spark-runtime-mode clear
```

The collector should treat IBKR as opportunistic:

- if IBKR is connected, use it as the best available source for paid exchange data
- if IBKR is disconnected or kicked by mobile trading, mark fields unavailable and continue from other providers
- never automatically fight the phone session
- store provider and quality flags with every derived feature

Fallback should be feature-level, not a global provider switch:

- SPXW option quotes and Greeks: IBKR first; Schwab if verified for SPX/SPXW; otherwise unavailable
- SPY/QQQ/IWM option quotes: Schwab first during protected manual trading; IBKR can cross-check inside its allowed window
- ES/MES: Schwab or IBKR when entitled; Hyperliquid SPX is context only, not a true ES replacement
- SPX/VIX/VVIX/SKEW: IBKR/Cboe when available; otherwise mark unavailable unless another verified vendor is added
- risk proxies: Schwab/IBKR ETF quotes are interchangeable if live and fresh
- chain/prediction signals: keep running independently regardless of broker state

Every feature row should include:

- `provider`
- `provider_priority`
- `quality`: `live`, `delayed`, `stale`, `missing`, or `synthetic`
- `quote_age_ms`
- `fallback_reason`

This avoids losing the dashboard and alert stream, but it does not create a complete IBKR/OPRA tick history for the hours when IBKR is intentionally offline. Complete OPRA tick retention requires either a non-conflicting broker/API source that works for SPXW, a second IBKR username with duplicate entitlements, or a dedicated vendor such as ThetaData/Databento.

## Data Priority

P0:

- SPX/SPXW or XSP option chain and selected L1 option quotes
- SPY/QQQ/IWM options if SPX is unavailable
- SPX cash, VIX family, VVIX, SKEW from IBKR/Cboe where available
- SPY, QQQ, IWM, HYG, LQD, TLT, IEF, SHY, UUP, GLD, USO
- Hyperliquid SPX context

P1:

- ES/MES from Schwab or IBKR when available
- Polymarket event markets
- TradeXYZ and on-chain smart-wallet research

P2:

- CBOT Treasury futures
- CFE VX futures
- paid OPRA tick feed vendor such as ThetaData or Databento if broker APIs are insufficient

## Sources

- Schwab streaming data overview: https://www.schwab.com/content/how-to-use-streaming-data
- Schwab futures markets: https://www.schwab.com/futures/futures-markets
- schwab-py HTTP client docs: https://schwab-py.readthedocs.io/en/latest/client.html
- schwab-py streaming docs: https://schwab-py.readthedocs.io/en/latest/streaming.html
- schwab-py auth docs: https://schwab-py.readthedocs.io/en/latest/auth.html
- Cboe SPX/SPXW specs: https://www.cboe.com/tradable-products/sp-500/spx-options/spx-specifications/
- IBKR TWS API docs: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
- IBKR market data subscriptions docs: https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/
