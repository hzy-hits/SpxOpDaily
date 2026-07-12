from __future__ import annotations

from spx_spark.marketdata import MarketDataQuality

BASELINE_INSTRUMENTS = (
    "index:SPX",
    "index:VIX",
    "index:VIX1D",
    "index:VIX9D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
    "index:NDX",
    "index:RUT",
    "index:DJI",
    "index:DJU",
    "equity:SPY",
    "equity:QQQ",
    "equity:IWM",
    "equity:DIA",
    "equity:HYG",
    "equity:LQD",
    "equity:TLT",
    "equity:IEF",
    "equity:SHY",
    "equity:UUP",
    "equity:GLD",
    "equity:USO",
    "equity:RSP",
    "equity:XLU",
    "future:ES",
    "future:MES",
    "crypto_perp:xyz:SP500",
)

MOVE_THRESHOLDS_BPS = {
    "critical": 20.0,
    "high": 30.0,
    "elevated": 45.0,
    "normal": 60.0,
    "low": 85.0,
    "off": 99999.0,
}

EM_MOVE_FRACTIONS = {
    "critical": 0.20,
    "high": 0.30,
    "elevated": 0.40,
    "normal": 0.50,
    "low": 0.35,
    "off": 9.0,
}

# In quiet (low-priority) windows the static 85 bps bar is unreachable in
# low-vol regimes, so overnight dips never alert. When expected move is known,
# scale the bar down to the EM fraction instead (floored to avoid tick noise).
QUIET_EM_THRESHOLD_FLOOR_BPS_DEFAULT = 15.0

# A move consuming this fraction of the day's expected move is escalated to
# high severity so it clears the notify gate even in low-priority windows.
# Kept equal to the quiet EM fraction so any move that crosses the quiet bar
# also clears the notify severity gate.
MOVE_HIGH_SEVERITY_EM_FRACTION_DEFAULT = 0.35

BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
    MarketDataQuality.DELAYED,
    MarketDataQuality.DELAYED_FROZEN,
}

OPTION_GAMMA_ALERT_STATES = {
    "negative_gamma_acceleration",
    "zero_gamma_transition",
}

BAD_SURFACE_QUALITIES = {
    "missing_options",
    "missing_atm_iv",
    "low_iv_coverage",
    "wide_quote_degraded",
}
BLOCKING_SURFACE_QUALITIES = {"missing_options", "missing_atm_iv"}
DEGRADED_SURFACE_QUALITIES = {"low_iv_coverage", "wide_quote_degraded"}
# Algorithm thresholds in absolute IV / skew units (not env-tunable identities).
ATM_IV_JUMP_THRESHOLD = 0.03  # 5-minute ATM IV jump that opens an IV-jump alert
SKEW_STEEPENING_THRESHOLD = 0.08  # 5-minute put-skew steepening alert floor
SKEW_25D_STEEPENING_THRESHOLD = 0.02  # default 25-delta skew steepening floor
SURFACE_SHIFT_THRESHOLD = 0.03  # 5-minute whole-surface level shift floor
TERM_GAP_THRESHOLD = 0.05  # front-vs-next ATM IV term-structure gap floor
SURFACE_SHIFT_1H_THRESHOLD = 0.05  # default 1-hour surface shift floor (env-overridable)
ATM_IV_CHANGE_1H_THRESHOLD = 0.04  # default 1-hour ATM IV change floor (env-overridable)
IBKR_INTERRUPTED_SESSION_STATUSES = {"competing_session", "unavailable"}
# Transitional statuses must not overwrite the persisted session status:
# "degraded" is what the stream collector reports between reconnect and the
# first flush, so persisting it would break the interrupted -> available
# transition and swallow the "restored" notification.
IBKR_TRANSITIONAL_SESSION_STATUSES = {"unknown", "degraded"}

# Walls recompute every cycle and drift a strike or two as OI updates; keying
# the cooldown on the exact strike turned every 5-point wall move into a fresh
# alert. Deduping by 25-point band keeps re-alerts for genuinely new levels only.
WALL_DEDUP_BAND_POINTS = 25.0


