# Unified Market Feature Frames

The feature pipeline converts provider-neutral `Quote` records and the existing
`OptionsMap` into three versioned projections every minute.

## Projections

- `latest/minute_market_frame.json`
  - ES 1/5/15/60/180-minute path, session and segment ranges, swing structure,
    trend efficiency, anchored VWAP, volume deltas, cross-asset confirmation,
    provider divergence and volatility context.
- `latest/option_structure_frame.json`
  - 0DTE/1DTE walls, wall migration, Max Pain, OI/volume/Gamma concentration,
    IV/skew/term structure, risk-neutral density changes and hot-option L1
    microstructure.
- `latest/decision_context.json`
  - Globex regime, mutually-exclusive wall/flip state, confirmations,
    invalidations and source-frame identifiers.

Durable calculation state is stored in `latest/market_feature_state.json`. The
persistent hot owner keeps the current five-second calculation state in memory
and checkpoints the completed state once per minute. Rolling samples contain
only feature inputs; exact BBO is always reloaded from `LatestState` at the
action boundary. Option history is a compact projection of the fields consumed
by rolling wall, IV, density and spread comparisons rather than full frames.
Material decision-context changes are appended to
`audit/decision_context/date=YYYY-MM-DD/events.jsonl`.

Operational strategy decisions remain full fidelity for selected candidates
and semantic changes. Identical `NO_TRADE` meaning is sampled once per minute;
`latest/strategy_decision.json` continues to expose the newest evaluation.
Due research outcome marks are checked once per minute (within their 90-second
causal window), while the five-second action path continues to reload exact
BBO. Historical spot/surface path repricing uses one NumPy path-by-time matrix
per leg instead of Python work per timestamp.
Core emits bounded alert and Steven cycle summaries; complete payloads remain
in their existing projections instead of being copied into journald.
The immutable storage/freshness policy is resolved once per process and reused
by per-contract quote checks; explicit settings passed by tests or callers
continue to take precedence.

## Availability Rules

- A source timestamp and transport timestamp must both pass the configured age
  gate before a quote enters cross-asset features.
- ES/SPX basis and Schwab/IBKR divergence require synchronized source times.
- Missing cash or second-provider data produces `null`/`unavailable`; the
  pipeline never substitutes a stale cash index or proxy.
- The same-clock volume percentile remains unavailable until 20 prior sessions
  are present. The frame publishes baseline sample count and readiness.
- OI is structural data. Intraday direction and flow use price, volume, quotes
  and IV changes rather than treating OI changes as new positions.

## Session Segments

Segment boundaries are typed settings in `config/runtime.yaml` and evaluated in
`America/New_York` time. The defaults are Asia through 03:00, Europe through
08:00, US premarket through 09:30, RTH through 16:00 and curb through 17:00.

## Decision Audit

`DecisionAudit` defines decision mid, order limit, fill, slippage and outcome
references. Fields remain null until an actual decision or broker execution can
provide them. Existing wall/flip outcome records are linked by event ID; no
synthetic execution values are generated.
