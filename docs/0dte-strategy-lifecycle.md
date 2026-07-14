# 0DTE strategy lifecycle

This document records the production decision boundaries for the SPX intraday
engine.  The implementation is advisory and never submits an order.

## Stable terrain map

Raw OI/GEX analytics remain in the audit payload.  The decision map promotes a
wall change only after three distinct 15-minute buckets agree on a materially
different structure.  Promoted Put Wall, flip and Call Wall values are
published as configurable bands, together with promotion time, duration and
confirmation count.  Active level events keep their frozen coordinate and ES
basis so a returning SPX/parity quote cannot invalidate a valid path merely by
changing the price source.

## ES-led GTH dip reclaim

The GTH fast lane needs a fresh live ES quote but does not require a direct
overnight SPX print.  It evaluates 15- and 60-minute peak-to-trough paths,
requires a minimum descent duration, a configured recovery fraction and a
60-second hold.  It also has a one-hour session warmup, one-hour cooldown and a
hard three-signal session ceiling.  A macro pre-event window or an already
active virtual strategy suppresses a new entry advisory.

## One primary strategy

Order-map presentation exposes either one TradeReady plan or one observation
strategy.  A candidate in the opposite direction is reduced to an invalidation
condition; it is never rendered as a concurrent plan.  Full raw candidates are
retained for research and replay.

## Greeks boundary

Gamma, Speed and Color describe remaining convexity; Theta and Charm describe
waiting cost; Vanna plus IV-down scenarios describe post-event crush risk.
They have no index-direction authority.  Only when exact-expiry usable and OI
coverage both meet the configured gate may they adjust same-direction contract
confidence or a virtual exit.  Otherwise the entire layer is explanation-only.

## Macro clock and virtual lifecycle

Scheduled releases are explicit records in `config/macro_events.yaml`; the
model cannot invent event times.  The clock reports normal, pre-event or
post-event mode.  The virtual lifecycle tracks the system's own Call episode
without IBKR positions, records entry/current Greeks and 5/15/30-minute
MFE/MAE, and can emit take-profit, reduce or exit advice for target/wall touch,
premium gain, Delta saturation, post-event IV/Vanna drag, Gamma/Color decay,
invalidation or time stop.  Every message states that account positions are
unknown and automatic ordering is disabled.
