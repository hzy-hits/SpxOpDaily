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

Durable calculation state is stored in `latest/market_feature_state.json`.
Material decision-context changes are appended to
`audit/decision_context/date=YYYY-MM-DD/events.jsonl`.

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
