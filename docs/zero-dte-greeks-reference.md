# SPXW 0DTE Greeks Reference

This feature is a versioned, read-only sensitivity layer for literal same-day
SPXW expiries. It does not rank candidates, choose Call versus Put, change
limits, or place orders.

## Scope

- Underlier: SPX.
- Contract class: SPXW.
- Expiry: the current New York calendar date, during that session's RTH only.
- No 1DTE fallback and no overnight or cross-day contract rows.
- ES, VIX1D, and the next expiry remain context elsewhere; they are not inputs
  to this Greek calculation.
- The last five minutes are blocked because higher-order finite differences
  are numerically unstable there.

The RTH spot anchor uses actionable cash SPX first, then co-fresh SPXW put-call
parity, then the median IBKR model underlier. ES/SPY basis is never silently
substituted. Anchor disagreement above 20 bps degrades the snapshot;
disagreement at or above 50 bps blocks the numerical reference.

## Model and units

Schema: `spxw_0dte_greeks_reference.v1`.

Model v1 is Black-Scholes with `r=0` and `q=0`. The model label is persisted so
historical snapshots cannot silently change meaning if a forward/rate-aware
model is introduced later.

- Delta: per option.
- Gamma: Delta change per one SPX point.
- Theta: option points per calendar minute.
- Vega: option points per one volatility point (`0.01 sigma`).
- Charm: Delta change per calendar minute.
- Color: Gamma change per calendar minute.
- Speed: Gamma change per SPX point.
- Vanna: Delta change per one volatility point.
- Vomma: option-price curvature per volatility-point squared.
- Zomma: Gamma change per one volatility point.

Charm and Color use forward calendar time: clock time increases while time to
expiry decreases. Speed, Vanna, Vomma, and Zomma use central differences and a
half-step stability check. Full contract calculation supports spot
`+/-0.25%` and `+/-0.5%`, clock `+5/+15/+30m`, and IV `+/-1/+/-3` vol-point
repricing. Live writer payloads retain aggregate and quality fields only, with
no contract scenario prices, so the shadow layer cannot influence candidate
ranking and recurring LLM tokens stay bounded. The full pure calculation
remains testable in code.

## Direction guardrail

OI-weighted aggregates are gross absolute magnitudes only. Volume is context,
not a position sign. Both `position_sign` and `direction` remain `unknown`.
The separate `signed_gex_proxy` uses calls positive and puts negative with
open-interest weighting. It is explicitly labeled
`call_positive_put_negative_oi_proxy_not_dealer_position`; dealer positioning
sign remains `unknown` and the proxy is never a directional vote.
Therefore:

- negative Gamma does not mean price must fall;
- gross Gamma is not signed dealer GEX;
- no Greek may independently turn a Call setup into a Put setup;
- a directional interpretation still requires SPX price acceptance plus live
  SPX/ES confirmation.

## Quality and persistence

Only pricing-allowed fresh option quotes enter the dynamic aggregate. The
payload separately reports total exact-expiry rows and the currently usable
subset so normal contract rotation is visible without admitting stale Greeks.
Wide spreads, vendor/model divergence, deep-wing Delta, unstable differences,
and anchor disagreement are labeled explicitly.

Successfully delivered order-map/status snapshots are written to:

- `data/features/spxw_0dte_greeks_reference/date=YYYY-MM-DD/snapshots.jsonl`
- `data/latest/spxw_0dte_greeks_reference.json`

With `SPX_SERVICE_ENABLE_GREEK_SHADOW=true`, the service loop records one
research-only RTH sample every 60 seconds. A shock or reclaim also forces an
event sample with its phase and synchronized SPX/ES context. Shadow samples
set notification, strategy action, and order placement permissions to `false`;
stale or mismatched data produces a blocked health record instead of a value.

The post-close review summarizes first, last, and peak gross sensitivities in a
separate `0DTE Greeks Reference` section. Missing snapshots do not downgrade the
existing report-completeness verdict because this layer begins in shadow mode.
