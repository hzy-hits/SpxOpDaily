"use strict";

const SNAPSHOT_URL = "api/v1/snapshot";
const REPLAY = Object.freeze({
  id: "2026-07-17T183500Z",
  label: "Replay · Fri 7/17 14:35 ET",
  url: "api/v1/replays/2026-07-17T183500Z",
  view: "friday",
  requestedAsOf: "2026-07-17T18:35:00Z",
  asOfLabel: "Fri 7/17 14:35 ET · 2026-07-17 18:35 UTC",
  policyVersion: "spxw_surface_replay.v2",
  sourceFile: "lake/quotes/schema=v1/date=2026-07-17/provider=schwab/hour=18/quotes.parquet",
  sourceSha256: "85dc310f97113cf106ba54ef5532f3a002c10e67cec069de9c8933924cac2dff",
  rawSourceFile: "raw/provider=schwab/date=2026-07-17/hour=18/quotes.jsonl",
  rawSourceSha256: "7e627e0d2f5415a48934196b4ab29d9448e21ead53aaa742d8b6da46b46fca43",
  projectionPolicySha256: "aa507d6490f5734c52c46a7f5b6763e4d1f402523a75cf1a5df0c6e6dd83dd55",
  artifactSha256: "475ba8eae84269f4fe453a231a438d2f47ff38a640dbfb2113e98b71210b0e0f",
});
const POLL_INTERVAL_MS = 5_000;
const REQUEST_TIMEOUT_MS = 4_500;
const SVG_NS = "http://www.w3.org/2000/svg";

const METRICS = {
  signed_gamma: {
    label: "Signed gamma",
    aliases: ["signed_gamma", "net_gex"],
    signed: true,
  },
  gross_gamma: {
    label: "Gross gamma",
    aliases: ["gross_gamma", "abs_gex"],
    signed: false,
  },
  charm: {
    label: "Charm",
    aliases: ["charm", "cex_proxy"],
    signed: true,
  },
  vanna: {
    label: "Vanna",
    aliases: ["vanna", "vex_proxy"],
    signed: true,
  },
};

const WEIGHTINGS = {
  oi_weighted: "OI · Signed proxy",
  volume_weighted: "Volume · Activity proxy",
};

const WEIGHTING_DESCRIPTIONS = {
  oi_weighted: "OI 结构代理；0 有效，缺失剔除",
  volume_weighted: "累计成交量活动代理（非买卖方向）；0 有效，缺失剔除",
};

const UNIT_LABELS = {
  proxy_delta_dollars_per_1pct_underlier_move: "proxy Δ$ / 1% SPX move",
  gross_delta_dollars_per_1pct_underlier_move: "gross Δ$ / 1% SPX move",
  proxy_1pct_notional_delta_change_per_calendar_minute: "proxy 1% notional Δ / calendar minute",
  proxy_1pct_notional_delta_change_per_1_vol_point: "proxy 1% notional Δ / 1 vol point",
};

const STATUS_LABELS = {
  ready: "Ready",
  degraded: "Degraded",
  unavailable: "Unavailable",
  unknown: "Unknown",
};

const REASON_LABELS = {
  unpaired_strike: "部分 strike 缺少 Call/Put 配对（unpaired_strike）",
  underlier_unavailable: "标的行情不可用（underlier_unavailable）",
};

const COLORS = {
  ink: "#17202a",
  muted: "#687482",
  faint: "#939da8",
  border: "#dce1e6",
  neutral: [242, 241, 237],
  positive: [47, 111, 173],
  positiveDark: "#174f84",
  negative: [217, 100, 89],
  negativeDark: "#a93d35",
  missing: "#f8f9fa",
};

const dom = {
  pageLede: document.querySelector("#page-lede"),
  replayBanner: document.querySelector("#replay-banner"),
  replayBannerAsOf: document.querySelector("#replay-banner-as-of"),
  statusPill: document.querySelector("#status-pill"),
  refreshState: document.querySelector("#refresh-state"),
  summaryStatus: document.querySelector("#summary-status"),
  summaryReasons: document.querySelector("#summary-reasons"),
  summaryFreshness: document.querySelector("#summary-freshness"),
  summaryAsOf: document.querySelector("#summary-as-of"),
  summaryCoverage: document.querySelector("#summary-coverage"),
  summaryContracts: document.querySelector("#summary-contracts"),
  summaryExpiries: document.querySelector("#summary-expiries"),
  summaryUnderlier: document.querySelector("#summary-underlier"),
  notice: document.querySelector("#data-notice"),
  modeFilter: document.querySelector("#mode-filter"),
  expiryFilter: document.querySelector("#expiry-filter"),
  weightingFilter: document.querySelector("#weighting-filter"),
  metricFilter: document.querySelector("#metric-filter"),
  surfaceTitle: document.querySelector("#surface-title"),
  surfaceSubtitle: document.querySelector("#surface-subtitle"),
  surfaceQuality: document.querySelector("#surface-quality"),
  heatmapStage: document.querySelector("#heatmap-stage"),
  heatmap: document.querySelector("#heatmap"),
  heatmapTooltip: document.querySelector("#heatmap-tooltip"),
  heatmapEmpty: document.querySelector("#heatmap-empty"),
  legendNegative: document.querySelector("#legend-negative"),
  legendNeutral: document.querySelector("#legend-neutral"),
  legendPositive: document.querySelector("#legend-positive"),
  legendRidge: document.querySelector("#legend-ridge"),
  legendPeak: document.querySelector("#legend-peak"),
  legendTrough: document.querySelector("#legend-trough"),
  legendDomain: document.querySelector("#legend-domain"),
  accessibleSummary: document.querySelector("#chart-accessible-summary"),
  ladderChart: document.querySelector("#ladder-chart"),
  ladderEmpty: document.querySelector("#ladder-empty"),
  ladderSubtitle: document.querySelector("#ladder-subtitle"),
  peakList: document.querySelector("#peak-list"),
  troughList: document.querySelector("#trough-list"),
  peakHeading: document.querySelector("#peak-heading"),
  troughHeading: document.querySelector("#trough-heading"),
  extremaSubtitle: document.querySelector("#extrema-subtitle"),
  sourceFile: document.querySelector("#source-file"),
  sourceMode: document.querySelector("#source-mode"),
  schemaVersion: document.querySelector("#schema-version"),
  signConvention: document.querySelector("#sign-convention"),
};

const app = {
  mode: initialModeFromQuery(),
  snapshot: null,
  expiry: "",
  weighting: "oi_weighted",
  metric: "signed_gamma",
  chartHit: null,
  timer: null,
  requestController: null,
  requestGeneration: 0,
};

function initialModeFromQuery() {
  return new URLSearchParams(window.location.search).get("view") === REPLAY.view
    ? "replay"
    : "live";
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function finiteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizedStatus(value) {
  const raw = isObject(value) ? value.status : value;
  const text = String(raw || "").toLowerCase();
  if (["ready", "ok", "available", "fresh"].includes(text)) return "ready";
  if (["degraded", "partial", "stale", "warning"].includes(text)) return "degraded";
  if (["unavailable", "blocked", "missing", "error", "insufficient"].includes(text)) {
    return "unavailable";
  }
  return "unknown";
}

function reasonsFrom(value) {
  if (!isObject(value)) return [];
  const candidates = [value.reasons, value.warnings];
  return candidates
    .flatMap((candidate) => (Array.isArray(candidate) ? candidate : []))
    .filter((item) => typeof item === "string" && item.trim())
    .map((item) => item.trim());
}

function reasonLabel(reason) {
  return REASON_LABELS[reason] || reason;
}

function canonicalReplayBytes(value) {
  const encoder = new TextEncoder();
  function encode(item) {
    if (item === null) return "n;";
    if (typeof item === "boolean") return item ? "b1;" : "b0;";
    if (typeof item === "number") {
      if (!Number.isFinite(item)) throw new Error("replay_hash_non_finite_number");
      if (Number.isInteger(item) && !Number.isSafeInteger(item)) {
        throw new Error("replay_hash_unsafe_integer");
      }
      const buffer = new ArrayBuffer(8);
      new DataView(buffer).setFloat64(0, item, false);
      const hex = Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
      return `f${hex};`;
    }
    if (typeof item === "string") {
      return `s${encoder.encode(item).length}:${item}`;
    }
    if (Array.isArray(item)) {
      return `a${item.length}[${item.map(encode).join("")}]`;
    }
    if (isObject(item)) {
      const keys = Object.keys(item).sort();
      return `o${keys.length}{${keys.map((key) => `${encode(key)}${encode(item[key])}`).join("")}}`;
    }
    throw new Error("replay_hash_unsupported_value");
  }
  return encoder.encode(encode(value));
}

async function canonicalReplaySha256(value) {
  if (!globalThis.crypto?.subtle) throw new Error("replay_crypto_unavailable");
  const digest = await globalThis.crypto.subtle.digest("SHA-256", canonicalReplayBytes(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function verifyReplayDigests(raw) {
  if (!isObject(raw.projection_policy)) throw new Error("missing_replay_projection_policy");
  const policyDigest = await canonicalReplaySha256(raw.projection_policy);
  if (
    policyDigest !== raw.projection_policy_sha256 ||
    policyDigest !== REPLAY.projectionPolicySha256
  ) {
    throw new Error("replay_projection_policy_hash_mismatch");
  }
  const artifactBody = { ...raw };
  delete artifactBody.artifact_sha256;
  const artifactDigest = await canonicalReplaySha256(artifactBody);
  if (artifactDigest !== raw.artifact_sha256 || artifactDigest !== REPLAY.artifactSha256) {
    throw new Error("replay_artifact_hash_mismatch");
  }
}

function parseDate(value) {
  const text = nonEmptyString(value);
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeSurface(raw) {
  if (!isObject(raw)) return null;
  const originalGrid = Array.isArray(raw.spot_grid)
    ? raw.spot_grid.map(finiteNumber)
    : Array.isArray(raw.spots)
      ? raw.spots.map(finiteNumber)
      : [];
  if (originalGrid.length < 2 || originalGrid.some((value) => value === null)) return null;

  const spotOrder = originalGrid
    .map((spot, index) => ({ spot, index }))
    .sort((left, right) => left.spot - right.spot);
  const spotGrid = spotOrder.map((item) => item.spot);
  if (spotGrid.some((spot, index) => index > 0 && spot <= spotGrid[index - 1])) return null;
  const sourceSlices = Array.isArray(raw.time_slices) ? raw.time_slices : [];
  const timeSlices = sourceSlices
    .filter(isObject)
    .map((slice, sourceIndex) => ({
      raw: slice,
      sourceIndex,
      minutesForward:
        finiteNumber(slice.minutes_forward) ??
        finiteNumber(Array.isArray(raw.time_offsets_minutes) ? raw.time_offsets_minutes[sourceIndex] : null),
      tauSeconds: finiteNumber(slice.tau_seconds),
      quality: normalizedStatus(slice.quality),
      warnings: [...reasonsFrom(slice.quality), ...reasonsFrom(slice)],
    }))
    .filter((slice) => slice.minutesForward !== null)
    .sort((left, right) => left.minutesForward - right.minutesForward);

  if (!timeSlices.length) return null;
  if (timeSlices.some((slice, index) => index > 0 && slice.minutesForward <= timeSlices[index - 1].minutesForward)) {
    return null;
  }
  return {
    raw,
    schemaVersion: nonEmptyString(raw.schema_version),
    asOf: parseDate(raw.as_of),
    expiry: nonEmptyString(raw.expiry),
    expiryClose: parseDate(raw.expiry_close),
    referenceSpot: finiteNumber(raw.reference_spot),
    spotGrid,
    spotOrder,
    timeSlices,
    quality: normalizedStatus(raw.quality),
    warnings: [...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    metricUnits: isObject(raw.metric_units) ? raw.metric_units : {},
    weightingSemantics: isObject(raw.weighting_semantics) ? raw.weighting_semantics : {},
    signConvention: nonEmptyString(raw.sign_convention),
    dealerPositionSign: nonEmptyString(raw.dealer_position_sign),
    model: nonEmptyString(raw.model),
  };
}

function normalizeExpiry(raw, index) {
  if (!isObject(raw)) return null;
  const surface = normalizeSurface(raw.surface || raw.exposure_surface || raw);
  const expiry = nonEmptyString(raw.expiry) || surface?.expiry;
  if (!expiry) return null;
  const role = nonEmptyString(raw.role) || (index === 0 ? "front" : index === 1 ? "next" : "other");
  const ladder = Array.isArray(raw.strike_ladder)
    ? raw.strike_ladder.filter(isObject)
    : Array.isArray(raw.ladder)
      ? raw.ladder.filter(isObject)
      : [];
  return {
    raw,
    expiry,
    role,
    surface,
    ladder,
    contractCount: finiteNumber(raw.contract_count) ?? finiteNumber(surface?.raw.contract_count),
    callCount: finiteNumber(raw.call_count),
    putCount: finiteNumber(raw.put_count),
    providers: Array.isArray(raw.providers)
      ? raw.providers.filter((item) => typeof item === "string")
      : [],
    quality: normalizedStatus(raw.quality || surface?.raw.quality),
    warnings: [...reasonsFrom(raw.quality), ...reasonsFrom(raw), ...(surface?.warnings || [])],
  };
}

function normalizeSnapshot(raw) {
  if (!isObject(raw)) throw new Error("snapshot_not_an_object");
  const kind = nonEmptyString(raw.kind);
  if (kind !== "spxw_surface_dashboard") throw new Error("unexpected_snapshot_kind");
  if (raw.schema_version !== 1) throw new Error("unsupported_snapshot_schema");
  if (raw.automatic_ordering !== false) throw new Error("unsafe_automatic_ordering_contract");

  const status = normalizedStatus(raw.status || raw.quality);
  if (status === "unknown") throw new Error("unsupported_snapshot_status");
  const createdAt = parseDate(raw.created_at);
  const asOf = parseDate(raw.as_of);
  const validUntil = parseDate(raw.valid_until);
  if (!createdAt || !asOf || !validUntil || validUntil.getTime() <= asOf.getTime()) {
    throw new Error("invalid_snapshot_clock_contract");
  }
  const expiryRows = Array.isArray(raw.expiries) ? raw.expiries : [];
  const expiries = status === "unavailable"
    ? []
    : expiryRows.map(normalizeExpiry).filter(Boolean);
  return {
    raw,
    mode: "live",
    kind,
    schemaVersion: String(raw.schema_version),
    status,
    createdAt,
    asOf,
    validUntil,
    automaticOrdering: raw.automatic_ordering === true,
    quality: isObject(raw.quality) ? raw.quality : {},
    reasons: [...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    underlier: isObject(raw.underlier) ? raw.underlier : {},
    session: isObject(raw.session) ? raw.session : {},
    expiries,
  };
}

async function normalizeReplaySnapshot(raw) {
  if (!isObject(raw)) throw new Error("replay_not_an_object");
  await verifyReplayDigests(raw);
  if (nonEmptyString(raw.kind) !== "spxw_surface_dashboard_replay") {
    throw new Error("unexpected_replay_kind");
  }
  if (raw.schema_version !== 1) throw new Error("unsupported_replay_schema");
  if (raw.mode !== "replay") throw new Error("invalid_replay_mode");
  if (raw.policy_version !== REPLAY.policyVersion) {
    throw new Error("unexpected_replay_policy_version");
  }
  if (raw.replay_id !== REPLAY.id) throw new Error("unexpected_replay_id");
  if (raw.frozen !== true) throw new Error("replay_must_be_frozen");
  if (raw.automatic_ordering !== false) throw new Error("unsafe_automatic_ordering_contract");
  if (Object.prototype.hasOwnProperty.call(raw, "valid_until")) {
    throw new Error("replay_must_not_have_valid_until");
  }
  if (Object.prototype.hasOwnProperty.call(raw, "created_at")) {
    throw new Error("replay_must_not_have_live_created_at");
  }
  if (raw.session_date !== "2026-07-17") throw new Error("unexpected_replay_session_date");

  const status = normalizedStatus(raw.status || raw.quality);
  if (status === "unknown") throw new Error("unsupported_replay_status");
  const requestedAsOf = parseDate(raw.requested_as_of);
  const dataAsOf = parseDate(raw.data_as_of);
  const generatedAt = parseDate(raw.generated_at);
  const expectedAsOf = parseDate(REPLAY.requestedAsOf);
  if (!requestedAsOf || !dataAsOf || !generatedAt || !expectedAsOf) {
    throw new Error("invalid_replay_clock_contract");
  }
  if (requestedAsOf.getTime() !== expectedAsOf.getTime()) {
    throw new Error("unexpected_replay_requested_as_of");
  }
  if (dataAsOf.getTime() > requestedAsOf.getTime()) {
    throw new Error("replay_data_after_requested_as_of");
  }
  if (generatedAt.getTime() < dataAsOf.getTime()) {
    throw new Error("replay_generated_before_data");
  }

  const source = isObject(raw.source) ? raw.source : null;
  if (!source) throw new Error("missing_replay_source_contract");
  const cutoffFields = [
    "received_at",
    "source_at",
    "quote_time",
    "trade_time",
    "last_update_at",
  ];
  if (
    !Array.isArray(source.cutoff_fields) ||
    source.cutoff_fields.length !== cutoffFields.length ||
    source.cutoff_fields.some((field, index) => field !== cutoffFields[index])
  ) {
    throw new Error("unsafe_replay_cutoff_fields");
  }
  if (
    source.cutoff_rule !==
    "received_at_and_available_source_clocks_lte_requested_as_of"
  ) {
    throw new Error("unsafe_replay_cutoff_rule");
  }
  if (source.lookahead_rows_selected !== 0) throw new Error("replay_lookahead_detected");
  if (source.coordinate !== "SPX") throw new Error("unsafe_replay_coordinate");
  if (source.trading_class !== "SPXW") throw new Error("unsafe_replay_trading_class");
  if (source.provider !== "schwab") throw new Error("unexpected_replay_provider");
  if (source.dataset !== "lake/quotes/schema=v1") throw new Error("unexpected_replay_dataset");
  if (source.lookback_seconds !== 15) throw new Error("unexpected_replay_lookback");
  if (
    source.selection_rule !==
    "latest_complete_row_per_instrument_by_available_clocks_then_surface_input_completeness"
  ) {
    throw new Error("unsafe_replay_selection_rule");
  }
  if (source.replay_loader_field_stitching !== false) {
    throw new Error("replay_field_stitching_detected");
  }
  if (source.source_files_verified_unchanged_during_read !== true) {
    throw new Error("unverified_replay_source_files");
  }
  if (
    source.structure_clock_available !== false ||
    source.compacted_at_available !== false ||
    !Array.isArray(source.compacted_at) ||
    source.compacted_at.length !== 0 ||
    !Array.isArray(source.lake_schema_versions) ||
    source.lake_schema_versions.length !== 1 ||
    source.lake_schema_versions[0] !== "v1" ||
    !Array.isArray(source.lake_writer_versions) ||
    source.lake_writer_versions.length !== 1 ||
    source.lake_writer_versions[0] !== "spx-spark-quote-compactor-v1"
  ) {
    throw new Error("unexpected_replay_lake_lineage");
  }
  const maxTransportAge = finiteNumber(source.max_transport_age_seconds);
  if (maxTransportAge === null || maxTransportAge < 0 || maxTransportAge > 15) {
    throw new Error("unsafe_replay_transport_age");
  }
  const maxObservationAge = finiteNumber(source.max_observation_age_seconds);
  const minObservationAge = finiteNumber(source.min_observation_age_seconds);
  if (
    maxObservationAge === null ||
    minObservationAge === null ||
    minObservationAge < 0 ||
    maxObservationAge < minObservationAge ||
    maxObservationAge > 15
  ) {
    throw new Error("unsafe_replay_observation_age");
  }
  const expiryCounts = isObject(source.selected_expiry_counts)
    ? source.selected_expiry_counts
    : {};
  if (
    source.selected_quote_count !== 241 ||
    expiryCounts["20260717"] !== 160 ||
    expiryCounts["20260720"] !== 80
  ) {
    throw new Error("unexpected_replay_quote_coverage");
  }
  if (
    source.raw_candidate_count !== 2106 ||
    source.eligible_candidate_count !== 2106 ||
    source.source_clock_rows_excluded !== 0 ||
    source.duplicate_received_at_group_count !== 480 ||
    source.duplicate_received_at_row_count !== 960 ||
    source.resolved_by_surface_completeness_instrument_count !== 35 ||
    source.ambiguous_top_instrument_count !== 0 ||
    source.dropped_ambiguous_instrument_count !== 0 ||
    source.identical_top_duplicate_row_count !== 0
  ) {
    throw new Error("unexpected_replay_selection_audit");
  }
  if (
    !Array.isArray(source.source_files) ||
    source.source_files.length !== 1 ||
    source.source_files[0] !== REPLAY.sourceFile
  ) {
    throw new Error("unexpected_replay_source_file");
  }
  const sourceHashes = isObject(source.parquet_file_sha256)
    ? source.parquet_file_sha256
    : {};
  if (
    Object.keys(sourceHashes).length !== 1 ||
    sourceHashes[REPLAY.sourceFile] !== REPLAY.sourceSha256
  ) {
    throw new Error("invalid_replay_source_hash");
  }
  const rawSourceHashes = isObject(source.raw_source_file_sha256)
    ? source.raw_source_file_sha256
    : {};
  if (
    Object.keys(rawSourceHashes).length !== 1 ||
    rawSourceHashes[REPLAY.rawSourceFile] !== REPLAY.rawSourceSha256
  ) {
    throw new Error("invalid_replay_raw_source_hash");
  }
  if (
    raw.projection_policy_sha256 !== REPLAY.projectionPolicySha256 ||
    raw.artifact_sha256 !== REPLAY.artifactSha256
  ) {
    throw new Error("invalid_replay_artifact_hash");
  }

  if (!Array.isArray(raw.expiries)) throw new Error("invalid_replay_expiries");
  const normalizedExpiries = raw.expiries.map(normalizeExpiry);
  if (normalizedExpiries.some((expiry) => !expiry)) throw new Error("invalid_replay_expiry");
  const expiries = status === "unavailable" ? [] : normalizedExpiries;
  return {
    raw,
    mode: "replay",
    kind: raw.kind,
    replayId: raw.replay_id,
    schemaVersion: String(raw.schema_version),
    status,
    createdAt: generatedAt,
    generatedAt,
    asOf: requestedAsOf,
    requestedAsOf,
    dataAsOf,
    validUntil: null,
    frozen: true,
    automaticOrdering: false,
    source,
    quality: isObject(raw.quality) ? raw.quality : {},
    reasons: [...reasonsFrom(raw.quality), ...reasonsFrom(raw)],
    underlier: isObject(raw.underlier) ? raw.underlier : {},
    session: isObject(raw.session) ? raw.session : {},
    expiries,
  };
}

function effectiveSnapshotStatus(snapshot) {
  if (!snapshot) return "unavailable";
  if (!["ready", "degraded"].includes(snapshot.status)) return "unavailable";
  if (snapshot.mode === "replay") {
    if (snapshot.frozen !== true || snapshot.validUntil !== null) return "unavailable";
    if (snapshot.source?.lookahead_rows_selected !== 0) return "unavailable";
    return snapshot.status;
  }
  if (!snapshot.validUntil) return "unavailable";
  if (snapshot.validUntil && Date.now() > snapshot.validUntil.getTime()) return "unavailable";
  return snapshot.status;
}

function selectedExpiry() {
  return app.snapshot?.expiries.find((item) => item.expiry === app.expiry) || null;
}

function metricArray(weighting, metric) {
  if (!isObject(weighting)) return null;
  const metrics = isObject(weighting.metrics) ? weighting.metrics : weighting;
  for (const key of METRICS[metric].aliases) {
    if (Array.isArray(metrics[key])) return metrics[key];
  }
  return null;
}

function metricScalar(container, metric) {
  if (!isObject(container)) return null;
  const metrics = isObject(container.metrics) ? container.metrics : container;
  for (const key of METRICS[metric].aliases) {
    const value = finiteNumber(metrics[key]);
    if (value !== null) return value;
  }
  return null;
}

function weightingFromSlice(slice, weightingKey) {
  const weightings = isObject(slice.raw.weightings) ? slice.raw.weightings : {};
  return isObject(weightings[weightingKey]) ? weightings[weightingKey] : null;
}

function surfaceView(expiry, weightingKey, metricKey) {
  const surface = expiry?.surface;
  if (!surface) return null;
  const rows = surface.timeSlices.map((slice) => {
    const weighting = weightingFromSlice(slice, weightingKey);
    const values = metricArray(weighting, metricKey);
    const reordered = surface.spotOrder.map(({ index }) => finiteNumber(values?.[index]));
    return {
      minutesForward: slice.minutesForward,
      tauSeconds: slice.tauSeconds,
      values: reordered,
      weighting,
      quality: normalizedStatus(weighting?.quality || slice.raw.quality),
      warnings: [...reasonsFrom(weighting?.quality), ...reasonsFrom(weighting), ...slice.warnings],
      zeroRidgeSpot: metricKey === "signed_gamma" ? finiteNumber(weighting?.zero_ridge_spot) : null,
      positivePeak: metricKey === "signed_gamma" && isObject(weighting?.positive_peak)
        ? {
            spot: finiteNumber(weighting.positive_peak.spot),
            value: finiteNumber(weighting.positive_peak.value),
          }
        : null,
      negativeTrough: metricKey === "signed_gamma" && isObject(weighting?.negative_trough)
        ? {
            spot: finiteNumber(weighting.negative_trough.spot),
            value: finiteNumber(weighting.negative_trough.value),
          }
        : null,
      coverage: isObject(weighting?.coverage) ? weighting.coverage : {},
      shapeValid:
        Array.isArray(values) &&
        values.length === surface.spotGrid.length &&
        values.every((value) => finiteNumber(value) !== null),
    };
  });
  const numericCount = rows.reduce(
    (total, row) => total + row.values.filter((value) => value !== null).length,
    0,
  );
  const invalidCount = metricKey === "gross_gamma"
    ? rows.reduce(
        (total, row) => total + row.values.filter((value) => value !== null && value < 0).length,
        0,
      )
    : 0;
  const shapeValid = rows.every((row) => row.shapeValid);
  return {
    surface,
    spots: surface.spotGrid,
    rows,
    numericCount,
    invalidCount,
    shapeValid,
    unit: metricUnit(surface, metricKey),
  };
}

function metricUnit(surface, metricKey) {
  for (const key of METRICS[metricKey].aliases) {
    const value = nonEmptyString(surface.metricUnits[key]);
    if (value) return UNIT_LABELS[value] || "model proxy units";
  }
  return "model units";
}

function qualityRank(status) {
  return { ready: 0, unknown: 1, degraded: 2, unavailable: 3 }[status] ?? 1;
}

function worstQuality(statuses) {
  return statuses.reduce(
    (worst, status) => (qualityRank(status) > qualityRank(worst) ? status : worst),
    "ready",
  );
}

function activeSurfaceQuality(expiry, view) {
  if (!expiry || !view || !view.shapeValid || view.numericCount === 0 || view.invalidCount > 0) {
    return "unavailable";
  }
  return worstQuality([
    expiry.quality,
    expiry.surface?.quality || "unknown",
    ...view.rows.map((row) => row.quality),
  ]);
}

function formatExpiry(value) {
  const text = String(value || "");
  if (!/^\d{8}$/.test(text)) return text || "—";
  const date = new Date(`${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}T12:00:00Z`);
  return new Intl.DateTimeFormat("zh-CN", { month: "short", day: "numeric" }).format(date);
}

function formatDateTime(date) {
  if (!(date instanceof Date)) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatIsoUtc(date) {
  if (!(date instanceof Date)) return "—";
  return date.toISOString().replace(".000Z", "Z");
}

function isReplayView() {
  return app.mode === "replay";
}

function updateModeQuery() {
  const url = new URL(window.location.href);
  if (isReplayView()) url.searchParams.set("view", REPLAY.view);
  else url.searchParams.delete("view");
  window.history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
}

function updateModeChrome() {
  const replay = isReplayView();
  document.body.classList.toggle("mode-replay", replay);
  dom.modeFilter.value = replay ? REPLAY.view : "live";
  dom.replayBanner.hidden = !replay;
  dom.pageLede.textContent = replay
    ? "标的价格 × 时间的期权暴露地形。当前为冻结历史回放，不是实时行情，也不提供下单入口。"
    : "标的价格 × 时间的期权暴露地形。所有值均来自只读生产快照，不提供下单入口。";
  dom.sourceFile.textContent = replay
    ? `replays/${REPLAY.id}.json`
    : "spxw_surface_dashboard.json";
  dom.sourceMode.textContent = replay ? "HISTORICAL REPLAY · Frozen · Not live" : "5 秒只读刷新";
  if (replay) {
    const dataAsOf = app.snapshot?.mode === "replay" ? formatIsoUtc(app.snapshot.dataAsOf) : "等待校验";
    dom.replayBannerAsOf.textContent = `As of ${REPLAY.asOfLabel} · data_as_of ${dataAsOf} · received/source clock cutoff · 0 selected lookahead rows`;
  }
}

function formatAge(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

function compactNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return "—";
  const absolute = Math.abs(value);
  const scales = [
    [1e12, "T"],
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "K"],
  ];
  for (const [scale, suffix] of scales) {
    if (absolute >= scale) return `${(value / scale).toFixed(digits)}${suffix}`;
  }
  if (absolute >= 100) return value.toFixed(0);
  if (absolute >= 1) return value.toFixed(digits);
  if (absolute === 0) return "0";
  return value.toExponential(2);
}

function ratioValue(coverage) {
  const explicit = finiteNumber(coverage.ratio);
  if (explicit !== null) return explicit;
  const usableRatio = finiteNumber(coverage.usable_ratio);
  if (usableRatio !== null) return usableRatio;
  const usable = finiteNumber(coverage.usable_contracts);
  const total = finiteNumber(coverage.total_contracts);
  return usable !== null && total && total > 0 ? usable / total : null;
}

function currentCoverage(view, expiry) {
  const nearest = view?.rows.reduce((best, row) => {
    if (!best) return row;
    return Math.abs(row.minutesForward) < Math.abs(best.minutesForward) ? row : best;
  }, null);
  const expiryCoverage = isObject(expiry?.raw?.coverage) ? expiry.raw.coverage : null;
  const coverage = expiryCoverage || nearest?.coverage || {};
  return {
    ratio: ratioValue(coverage),
    usable: finiteNumber(coverage.usable_contracts),
    total: finiteNumber(coverage.total_contracts) ?? expiry?.contractCount,
  };
}

function setStatusPill(status, label = null) {
  const normalized = STATUS_LABELS[status] ? status : "unknown";
  dom.statusPill.className = `status-pill status-${normalized}`;
  dom.statusPill.textContent = label || STATUS_LABELS[normalized];
}

function setQualityChip(status) {
  const normalized = STATUS_LABELS[status] ? status : "unknown";
  dom.surfaceQuality.className = `quality-chip quality-${normalized}`;
  dom.surfaceQuality.textContent = STATUS_LABELS[normalized];
}

function setNotice(message, isError = false) {
  dom.notice.hidden = !message;
  dom.notice.textContent = message || "";
  dom.notice.classList.toggle("error", isError);
}

function updateFilters() {
  const expiries = app.snapshot?.expiries || [];
  const previous = app.expiry;
  dom.expiryFilter.replaceChildren();
  if (!expiries.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "无可用到期日";
    dom.expiryFilter.append(option);
    app.expiry = "";
  } else {
    for (const expiry of expiries) {
      const option = document.createElement("option");
      option.value = expiry.expiry;
      const role = expiry.role === "front" ? "FRONT" : expiry.role === "next" ? "NEXT" : expiry.role.toUpperCase();
      option.textContent = `${role} · ${formatExpiry(expiry.expiry)} · ${expiry.expiry}`;
      dom.expiryFilter.append(option);
    }
    const retained = expiries.some((item) => item.expiry === previous);
    app.expiry = retained
      ? previous
      : (expiries.find((item) => item.role === "front") || expiries[0]).expiry;
    dom.expiryFilter.value = app.expiry;
  }
  const enabled = expiries.length > 0 && ["ready", "degraded"].includes(effectiveSnapshotStatus(app.snapshot));
  dom.expiryFilter.disabled = !enabled;
  dom.weightingFilter.disabled = !enabled;
  dom.metricFilter.disabled = !enabled;
  dom.weightingFilter.value = app.weighting;
  dom.metricFilter.value = app.metric;
}

function renderSummary() {
  const snapshot = app.snapshot;
  if (!snapshot) return;
  const replay = isReplayView() && snapshot.mode === "replay";
  const status = effectiveSnapshotStatus(snapshot);
  const expired = !replay && snapshot.validUntil && Date.now() > snapshot.validUntil.getTime();
  setStatusPill(status, replay ? `Replay · ${STATUS_LABELS[status]}` : expired ? "Stale" : null);
  dom.summaryStatus.textContent = replay
    ? `${STATUS_LABELS[status]} · Frozen replay`
    : expired ? "Stale / fail closed" : STATUS_LABELS[status];
  const expiry = selectedExpiry();
  const activeReasons = [...new Set([...snapshot.reasons, ...(expiry?.warnings || [])])];
  const reasons = expired ? ["snapshot_valid_until_elapsed", ...activeReasons] : activeReasons;
  const displayedReasons = reasons.map(reasonLabel);
  dom.summaryReasons.textContent = displayedReasons.slice(0, 3).join(" · ") || "无质量警告";

  const ageSeconds = snapshot.asOf ? Math.max((Date.now() - snapshot.asOf.getTime()) / 1000, 0) : null;
  dom.summaryFreshness.textContent = replay
    ? "Frozen · Not live"
    : expired ? `过期 ${formatAge(ageSeconds)}` : formatAge(ageSeconds);
  dom.summaryAsOf.textContent = replay
    ? `as of ${REPLAY.asOfLabel}`
    : `as of ${formatDateTime(snapshot.asOf)}`;

  const view = surfaceView(expiry, app.weighting, app.metric);
  const coverage = currentCoverage(view, expiry);
  dom.summaryCoverage.textContent = coverage.ratio === null ? "—" : `${(coverage.ratio * 100).toFixed(1)}%`;
  dom.summaryContracts.textContent = coverage.usable === null
    ? `合约 ${coverage.total ?? "—"}`
    : `可用 ${coverage.usable} / ${coverage.total ?? "—"}`;

  const front = snapshot.expiries.find((item) => item.role === "front");
  const next = snapshot.expiries.find((item) => item.role === "next");
  dom.summaryExpiries.textContent = front
    ? `${formatExpiry(front.expiry)}${next ? ` → ${formatExpiry(next.expiry)}` : ""}`
    : "—";
  const underlierPrice = finiteNumber(snapshot.underlier.price);
  const underlierSource = nonEmptyString(snapshot.underlier.source) || "unknown source";
  dom.summaryUnderlier.textContent = underlierPrice === null
    ? `SPX — · ${underlierSource}`
    : `SPX ${underlierPrice.toFixed(2)} · ${underlierSource}`;

  dom.schemaVersion.textContent = `schema ${snapshot.schemaVersion}`;
  const sign = expiry?.surface?.signConvention === "calls_positive_puts_negative"
    ? "calls + / puts − proxy"
    : "sign convention unavailable";
  const dealer = expiry?.surface?.dealerPositionSign === "unknown"
    ? "dealer side unknown"
    : "dealer side not asserted";
  dom.signConvention.textContent = `${sign}; ${dealer}`;
  dom.refreshState.textContent = replay
    ? `Frozen replay · loaded ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`
    : `最近检查 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  updateModeChrome();

  if (expired) {
    setNotice("快照已超过 valid_until，所有曲面已清空；等待 publisher 产生新数据。", true);
  } else if (status === "unavailable") {
    setNotice(`${replay ? "历史回放" : "生产快照"}不可用${displayedReasons.length ? `：${displayedReasons.slice(0, 4).join(" · ")}` : ""}`, true);
  } else if (status === "degraded") {
    setNotice(`${replay ? "历史回放为降级视图" : "当前为降级视图"}${displayedReasons.length ? `：${displayedReasons.slice(0, 4).join(" · ")}` : ""}`);
  } else if (displayedReasons.length) {
    setNotice(`${replay ? "历史回放质量提示" : "当前数据质量提示"}：${displayedReasons.slice(0, 4).join(" · ")}`);
  } else {
    setNotice("");
  }
}

function render() {
  updateFilters();
  renderSummary();
  renderVisuals();
}

function renderVisuals() {
  const snapshot = app.snapshot;
  const replay = isReplayView() && snapshot?.mode === "replay";
  const expiry = selectedExpiry();
  const metric = METRICS[app.metric];
  const view = surfaceView(expiry, app.weighting, app.metric);
  const snapshotStatus = effectiveSnapshotStatus(snapshot);
  const quality = snapshotStatus === "unavailable" ? "unavailable" : activeSurfaceQuality(expiry, view);
  setQualityChip(quality);

  const role = expiry?.role === "front" ? "Front" : expiry?.role === "next" ? "Next" : "Expiry";
  dom.surfaceTitle.textContent = expiry
    ? `${replay ? "Replay · " : ""}${role} ${expiry.expiry} · ${metric.label}`
    : "Spot × Time surface";
  const semanticsText = WEIGHTING_DESCRIPTIONS[app.weighting] || WEIGHTINGS[app.weighting];
  dom.surfaceSubtitle.textContent = expiry
    ? `${replay ? `HISTORICAL REPLAY · Frozen · Not live · as of ${REPLAY.asOfLabel} · ` : ""}${semanticsText} · X: SPX scenario spot · Y: minutes forward · ${view?.unit || "model units"}`
    : replay ? "等待冻结历史回放" : "等待生产快照";

  const available = snapshotStatus !== "unavailable" && quality !== "unavailable" && view?.numericCount > 0;
  dom.heatmapEmpty.hidden = available;
  if (!available) {
    clearCanvas();
    clearLadder();
    renderExtrema(null);
    updateLegend(null);
    dom.accessibleSummary.textContent = "当前没有可用的曲面数据。";
    return;
  }

  drawHeatmap(view);
  drawLadder(expiry, view);
  renderExtrema(view);
}

function clearCanvas() {
  const context = dom.heatmap.getContext("2d");
  context.clearRect(0, 0, dom.heatmap.width, dom.heatmap.height);
  app.chartHit = null;
  dom.heatmapTooltip.hidden = true;
}

function resizeCanvas(canvas) {
  const width = Math.max(canvas.clientWidth, 320);
  const height = Math.max(canvas.clientHeight, 360);
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  const pixelWidth = Math.round(width * ratio);
  const pixelHeight = Math.round(height * ratio);
  if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
    canvas.width = pixelWidth;
    canvas.height = pixelHeight;
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { context, width, height };
}

function extent(values) {
  const numeric = values.filter((value) => value !== null && Number.isFinite(value));
  if (!numeric.length) return { min: 0, max: 0, domain: 0 };
  const min = Math.min(...numeric);
  const max = Math.max(...numeric);
  return { min, max, domain: Math.max(Math.abs(min), Math.abs(max)) };
}

function mixColor(from, to, amount) {
  const ratio = Math.max(0, Math.min(amount, 1));
  return `rgb(${from.map((value, index) => Math.round(value + (to[index] - value) * ratio)).join(" ")})`;
}

function cellColor(value, domain, signed) {
  if (value === null) return COLORS.missing;
  if (domain <= 0) return mixColor(COLORS.neutral, COLORS.neutral, 0);
  if (!signed) return mixColor(COLORS.neutral, COLORS.positive, Math.max(value, 0) / domain);
  if (value < 0) return mixColor(COLORS.neutral, COLORS.negative, Math.abs(value) / domain);
  return mixColor(COLORS.neutral, COLORS.positive, value / domain);
}

function drawHeatmap(view) {
  const { context, width, height } = resizeCanvas(dom.heatmap);
  context.clearRect(0, 0, width, height);
  const mobile = width < 620;
  const margins = { left: mobile ? 56 : 72, right: 22, top: 24, bottom: 56 };
  const plotWidth = Math.max(width - margins.left - margins.right, 1);
  const plotHeight = Math.max(height - margins.top - margins.bottom, 1);
  const cellWidth = plotWidth / view.spots.length;
  const cellHeight = plotHeight / view.rows.length;
  const allValues = view.rows.flatMap((row) => row.values);
  const bounds = extent(allValues);
  const signed = METRICS[app.metric].signed;
  const domain = signed ? bounds.domain : Math.max(bounds.max, 0);

  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, width, height);
  for (let rowIndex = 0; rowIndex < view.rows.length; rowIndex += 1) {
    const row = view.rows[rowIndex];
    for (let column = 0; column < view.spots.length; column += 1) {
      const value = row.values[column];
      const x = margins.left + column * cellWidth;
      const y = margins.top + rowIndex * cellHeight;
      context.fillStyle = cellColor(value, domain, signed);
      context.fillRect(x, y, Math.ceil(cellWidth) + 0.25, Math.ceil(cellHeight) + 0.25);
      context.strokeStyle = "rgba(23, 32, 42, 0.07)";
      context.lineWidth = 0.5;
      context.strokeRect(x, y, cellWidth, cellHeight);

      if (value === null) {
        context.beginPath();
        context.moveTo(x + 2, y + cellHeight - 2);
        context.lineTo(x + Math.min(cellWidth - 2, cellHeight - 2), y + 2);
        context.strokeStyle = "rgba(104, 116, 130, 0.28)";
        context.stroke();
      } else if (cellWidth >= 22 && cellHeight >= 18) {
        context.fillStyle = "rgba(23, 32, 42, 0.7)";
        context.font = "700 10px ui-monospace, monospace";
        context.textAlign = "center";
        context.textBaseline = "middle";
        const glyph = signed
          ? value > 0 ? "+" : value < 0 ? "−" : "0"
          : value > 0 ? "•" : "0";
        context.fillText(glyph, x + cellWidth / 2, y + cellHeight / 2);
      }
    }
  }

  drawAxes(context, view, margins, plotWidth, plotHeight, width, height);
  if (app.metric === "signed_gamma") {
    drawSignedOverlays(context, view, margins, cellWidth, cellHeight);
  }
  updateLegend({ domain, signed, unit: view.unit });

  app.chartHit = { view, margins, plotWidth, plotHeight, cellWidth, cellHeight, width, height };
  const coverage = currentCoverage(view, selectedExpiry());
  dom.accessibleSummary.textContent = `${isReplayView() ? `历史回放，Frozen，Not live，as of ${REPLAY.asOfLabel}。` : ""}${METRICS[app.metric].label} 曲面，${view.spots.length} 个 spot 场景，${view.rows.length} 个时间切片，覆盖率 ${coverage.ratio === null ? "未知" : `${(coverage.ratio * 100).toFixed(1)}%`}，色域 ${signed ? `正负对称 ±${compactNumber(domain)}` : `0 到 ${compactNumber(domain)}`}。`;
}

function drawAxes(context, view, margins, plotWidth, plotHeight, width, height) {
  context.strokeStyle = COLORS.ink;
  context.lineWidth = 1;
  context.beginPath();
  context.moveTo(margins.left, margins.top + plotHeight + 0.5);
  context.lineTo(margins.left + plotWidth, margins.top + plotHeight + 0.5);
  context.moveTo(margins.left - 0.5, margins.top);
  context.lineTo(margins.left - 0.5, margins.top + plotHeight);
  context.stroke();

  context.fillStyle = COLORS.muted;
  context.font = "11px ui-monospace, SFMono-Regular, monospace";
  context.textBaseline = "top";
  const targetTickCount = plotWidth < 520 ? 4 : 7;
  const xStep = Math.max(Math.ceil((view.spots.length - 1) / (targetTickCount - 1)), 1);
  const tickIndexes = [];
  for (let index = 0; index < view.spots.length; index += xStep) {
    tickIndexes.push(index);
  }
  if (tickIndexes.at(-1) !== view.spots.length - 1) tickIndexes.push(view.spots.length - 1);
  for (const index of tickIndexes) {
    const x = margins.left + (index + 0.5) * (plotWidth / view.spots.length);
    context.textAlign = "center";
    context.fillText(formatSpotAxis(view.spots[index]), x, margins.top + plotHeight + 8);
  }

  const yStep = Math.max(Math.ceil(view.rows.length / 6), 1);
  context.textBaseline = "middle";
  for (let index = 0; index < view.rows.length; index += yStep) {
    const y = margins.top + (index + 0.5) * (plotHeight / view.rows.length);
    context.textAlign = "right";
    context.fillText(formatMinutes(view.rows[index].minutesForward), margins.left - 9, y);
  }

  context.fillStyle = COLORS.ink;
  context.font = "600 11px ui-sans-serif, system-ui, sans-serif";
  context.textAlign = "center";
  context.textBaseline = "bottom";
  context.fillText("SPX scenario spot", margins.left + plotWidth / 2, height - 5);
  context.save();
  context.translate(14, margins.top + plotHeight / 2);
  context.rotate(-Math.PI / 2);
  context.fillText("Minutes forward", 0, 0);
  context.restore();

  context.strokeStyle = "rgba(23, 32, 42, 0.08)";
  context.setLineDash([2, 5]);
  context.beginPath();
  context.moveTo(margins.left, margins.top + plotHeight / 2);
  context.lineTo(width - margins.right, margins.top + plotHeight / 2);
  context.stroke();
  context.setLineDash([]);
}

function xForSpot(spot, spots, margins, cellWidth) {
  if (!Number.isFinite(spot) || spot < spots[0] || spot > spots.at(-1)) return null;
  let right = spots.findIndex((candidate) => candidate >= spot);
  if (right <= 0) return margins.left + cellWidth / 2;
  if (right === -1) right = spots.length - 1;
  const left = right - 1;
  const span = spots[right] - spots[left];
  const fraction = span > 0 ? (spot - spots[left]) / span : 0;
  return margins.left + (left + 0.5 + fraction) * cellWidth;
}

function drawSignedOverlays(context, view, margins, cellWidth, cellHeight) {
  const ridge = view.rows
    .map((row, index) => ({
      x: xForSpot(row.zeroRidgeSpot, view.spots, margins, cellWidth),
      y: margins.top + (index + 0.5) * cellHeight,
    }))
    .filter((point) => point.x !== null);
  if (ridge.length) {
    context.strokeStyle = "#273542";
    context.lineWidth = 1.7;
    context.setLineDash([6, 5]);
    context.beginPath();
    ridge.forEach((point, index) => {
      if (index === 0) context.moveTo(point.x, point.y);
      else context.lineTo(point.x, point.y);
    });
    context.stroke();
    context.setLineDash([]);
    context.fillStyle = "#ffffff";
    context.strokeStyle = "#273542";
    for (const point of ridge) {
      context.beginPath();
      context.rect(point.x - 3, point.y - 3, 6, 6);
      context.fill();
      context.stroke();
    }
  }

  view.rows.forEach((row, index) => {
    drawExtremaMarker(context, row.positivePeak, "+", COLORS.positiveDark, index, view, margins, cellWidth, cellHeight);
    drawExtremaMarker(context, row.negativeTrough, "−", COLORS.negativeDark, index, view, margins, cellWidth, cellHeight);
  });
}

function drawExtremaMarker(context, point, glyph, color, rowIndex, view, margins, cellWidth, cellHeight) {
  if (!point || point.spot === null || point.value === null) return;
  const x = xForSpot(point.spot, view.spots, margins, cellWidth);
  if (x === null) return;
  const y = margins.top + (rowIndex + 0.5) * cellHeight;
  context.fillStyle = "rgba(255, 255, 255, 0.92)";
  context.strokeStyle = color;
  context.lineWidth = 1.6;
  context.beginPath();
  context.arc(x, y, 7, 0, Math.PI * 2);
  context.fill();
  context.stroke();
  context.fillStyle = color;
  context.font = "800 10px ui-monospace, monospace";
  context.textAlign = "center";
  context.textBaseline = "middle";
  context.fillText(glyph, x, y + 0.5);
}

function updateLegend(domainState) {
  const metric = METRICS[app.metric];
  const signedGamma = app.metric === "signed_gamma";
  dom.legendNegative.hidden = !metric.signed;
  dom.legendRidge.hidden = !signedGamma;
  dom.legendPeak.hidden = !signedGamma;
  dom.legendTrough.hidden = !signedGamma;
  setLegendText(dom.legendNeutral, metric.signed ? "中性" : "0 / 较低");
  setLegendText(dom.legendPositive, metric.signed ? "正暴露" : "较高 gross gamma");
  setLegendGlyph(dom.legendPositive, metric.signed ? "+" : "•");
  if (!domainState) {
    dom.legendDomain.textContent = "色域 —";
  } else if (domainState.signed) {
    dom.legendDomain.textContent = `对称色域 −${compactNumber(domainState.domain)} ↔ +${compactNumber(domainState.domain)} ${domainState.unit}`;
  } else {
    dom.legendDomain.textContent = `顺序色域 0 → ${compactNumber(domainState.domain)} ${domainState.unit}`;
  }
}

function setLegendText(element, value) {
  const textNode = Array.from(element.childNodes).find((node) => node.nodeType === Node.TEXT_NODE);
  if (textNode) textNode.textContent = ` ${value}`;
}

function setLegendGlyph(element, value) {
  const glyph = element.querySelector("b");
  if (glyph) glyph.textContent = value;
}

function formatMinutes(minutes) {
  if (!Number.isFinite(minutes)) return "—";
  if (minutes === 0) return "Now";
  if (Math.abs(minutes) < 60) return `${minutes > 0 ? "+" : ""}${minutes}m`;
  const hours = minutes / 60;
  return `${minutes > 0 ? "+" : ""}${Number.isInteger(hours) ? hours : hours.toFixed(1)}h`;
}

function formatSpotAxis(spot) {
  if (!Number.isFinite(spot)) return "—";
  return Number.isInteger(spot) ? String(spot) : spot.toFixed(1);
}

function tooltipForHit(hit, event) {
  const rect = dom.heatmap.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const column = Math.floor((x - hit.margins.left) / hit.cellWidth);
  const rowIndex = Math.floor((y - hit.margins.top) / hit.cellHeight);
  if (column < 0 || column >= hit.view.spots.length || rowIndex < 0 || rowIndex >= hit.view.rows.length) {
    dom.heatmapTooltip.hidden = true;
    return;
  }
  const row = hit.view.rows[rowIndex];
  const value = row.values[column];
  const title = document.createElement("strong");
  title.textContent = `SPX ${hit.view.spots[column].toFixed(2)} · ${formatMinutes(row.minutesForward)}`;
  const metric = document.createElement("span");
  metric.textContent = `${METRICS[app.metric].label}: ${value === null ? "Unavailable" : compactNumber(value, 3)} ${hit.view.unit}`;
  const quality = document.createElement("span");
  quality.textContent = `quality: ${STATUS_LABELS[row.quality] || row.quality}`;
  dom.heatmapTooltip.replaceChildren(title, metric, quality);
  dom.heatmapTooltip.hidden = false;
  const tooltipWidth = 190;
  const left = Math.min(Math.max(x + 13, 8), Math.max(rect.width - tooltipWidth - 8, 8));
  const top = Math.min(Math.max(y + 13, 8), Math.max(rect.height - 88, 8));
  dom.heatmapTooltip.style.left = `${left}px`;
  dom.heatmapTooltip.style.top = `${top}px`;
}

function ladderMetricValue(row, weightingKey, metricKey) {
  const weightings = isObject(row.weightings) ? row.weightings : {};
  const weighting = isObject(weightings[weightingKey])
    ? weightings[weightingKey]
    : isObject(row[weightingKey])
      ? row[weightingKey]
      : null;
  return metricScalar(weighting, metricKey);
}

function ladderContext(row) {
  const call = isObject(row.call) ? row.call : {};
  const put = isObject(row.put) ? row.put : {};
  const callOi = finiteNumber(call.open_interest);
  const putOi = finiteNumber(put.open_interest);
  const callVolume = finiteNumber(call.volume);
  const putVolume = finiteNumber(put.volume);
  return `Call OI ${callOi ?? "—"}, Put OI ${putOi ?? "—"}, Call volume ${callVolume ?? "—"}, Put volume ${putVolume ?? "—"}`;
}

function drawLadder(expiry, view) {
  const rows = (expiry?.ladder || [])
    .map((row) => ({
      raw: row,
      strike: finiteNumber(row.strike),
      value: ladderMetricValue(row, app.weighting, app.metric),
    }))
    .filter((row) => row.strike !== null && row.value !== null)
    .sort((left, right) => left.strike - right.strike);
  if (!rows.length) {
    clearLadder();
    return;
  }

  const reference = finiteNumber(expiry.surface?.referenceSpot) ?? finiteNumber(app.snapshot?.underlier.price);
  let selected = rows;
  const limit = 17;
  if (rows.length > limit) {
    const anchor = reference ?? rows[Math.floor(rows.length / 2)].strike;
    selected = rows
      .slice()
      .sort((left, right) => Math.abs(left.strike - anchor) - Math.abs(right.strike - anchor))
      .slice(0, limit)
      .sort((left, right) => left.strike - right.strike);
  }

  dom.ladderEmpty.hidden = true;
  dom.ladderSubtitle.textContent = `${selected.length} 个真实合约 strike${reference === null ? "" : ` · reference SPX ${reference.toFixed(2)}`} · ${view.unit}`;
  const width = Math.max(dom.ladderChart.clientWidth, 420);
  const rowHeight = 21;
  const height = 38 + selected.length * rowHeight;
  const labelWidth = 62;
  const valueWidth = 82;
  const chartLeft = labelWidth + 8;
  const chartRight = width - valueWidth - 8;
  const chartWidth = Math.max(chartRight - chartLeft, 100);
  const signed = METRICS[app.metric].signed;
  const bounds = extent(selected.map((row) => row.value));
  const domain = signed ? bounds.domain : Math.max(bounds.max, 0);
  const zeroX = signed ? chartLeft + chartWidth / 2 : chartLeft;
  dom.ladderChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  dom.ladderChart.setAttribute("height", String(height));
  dom.ladderChart.replaceChildren();

  appendSvg("line", {
    x1: zeroX,
    y1: 18,
    x2: zeroX,
    y2: height - 8,
    stroke: COLORS.ink,
    "stroke-width": 1,
    "stroke-dasharray": "3 4",
  });

  selected.forEach((row, index) => {
    const y = 24 + index * rowHeight;
    const ratio = domain > 0 ? Math.min(Math.abs(row.value) / domain, 1) : 0;
    const maxBar = signed ? chartWidth / 2 : chartWidth;
    const barWidth = ratio * maxBar;
    const x = signed && row.value < 0 ? zeroX - barWidth : zeroX;
    const fill = row.value < 0 ? "#d96459" : "#2f6fad";
    const rect = appendSvg("rect", {
      x,
      y,
      width: Math.max(barWidth, row.value === 0 ? 1 : 0),
      height: 13,
      rx: 2,
      fill,
      opacity: row.value === 0 ? 0.45 : 0.88,
    });
    const title = document.createElementNS(SVG_NS, "title");
    title.textContent = `${row.strike}: ${compactNumber(row.value, 3)} ${view.unit}; ${ladderContext(row.raw)}`;
    rect.append(title);
    appendSvgText(5, y + 10, row.strike.toFixed(0), "start", "#17202a", "11px");
    appendSvgText(width - 5, y + 10, compactNumber(row.value, 2), "end", row.value < 0 ? COLORS.negativeDark : COLORS.positiveDark, "11px");
  });
}

function appendSvg(tag, attributes) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [name, value] of Object.entries(attributes)) node.setAttribute(name, String(value));
  dom.ladderChart.append(node);
  return node;
}

function appendSvgText(x, y, value, anchor, fill, size) {
  const text = appendSvg("text", {
    x,
    y,
    fill,
    "font-size": size,
    "font-family": "ui-monospace, SFMono-Regular, Menlo, monospace",
    "text-anchor": anchor,
  });
  text.textContent = value;
}

function clearLadder() {
  dom.ladderChart.replaceChildren();
  dom.ladderEmpty.hidden = false;
  dom.ladderSubtitle.textContent = "快照未提供可用的真实 strike ladder";
}

function renderExtrema(view) {
  if (!view) {
    setExtremaHeadings();
    renderExtremaList(dom.peakList, [], "暂无数据");
    renderExtremaList(dom.troughList, [], "暂无数据");
    return;
  }
  setExtremaHeadings();
  const points = view.rows.flatMap((row) =>
    row.values.map((value, index) => ({
      value,
      spot: view.spots[index],
      minutesForward: row.minutesForward,
    })),
  ).filter((point) => point.value !== null);
  if (app.metric === "gross_gamma") {
    const highest = points.slice().sort((a, b) => b.value - a.value).slice(0, 5);
    const lowest = points.slice().sort((a, b) => a.value - b.value).slice(0, 5);
    renderExtremaList(dom.peakList, highest, "暂无 gross gamma 数据");
    renderExtremaList(dom.troughList, lowest, "暂无 gross gamma 数据");
    return;
  }
  const positives = points.filter((point) => point.value > 0).sort((a, b) => b.value - a.value).slice(0, 5);
  const negatives = points.filter((point) => point.value < 0).sort((a, b) => a.value - b.value).slice(0, 5);
  renderExtremaList(dom.peakList, positives, "没有正值");
  renderExtremaList(
    dom.troughList,
    negatives,
    METRICS[app.metric].signed ? "没有负值" : "Gross gamma 为非负指标",
  );
}

function setExtremaHeadings() {
  const gross = app.metric === "gross_gamma";
  dom.peakHeading.replaceChildren();
  dom.troughHeading.replaceChildren();
  if (gross) {
    dom.peakHeading.textContent = "Highest gross gamma";
    dom.troughHeading.textContent = "Lowest gross gamma";
    dom.extremaSubtitle.textContent = "Gross gamma 为非负质量指标；分别列出最高/最低值，不推断 dealer 方向。";
    return;
  }
  dom.extremaSubtitle.textContent = "按所选指标绝对幅度排序；数值不代表已知 dealer 实仓方向。";
  const positive = document.createElement("span");
  positive.className = "extrema-symbol positive";
  positive.textContent = "+";
  const negative = document.createElement("span");
  negative.className = "extrema-symbol negative";
  negative.textContent = "−";
  dom.peakHeading.append(positive, document.createTextNode(" Positive peaks"));
  dom.troughHeading.append(negative, document.createTextNode(" Negative troughs"));
}

function renderExtremaList(container, points, emptyLabel) {
  container.replaceChildren();
  if (!points.length) {
    const item = document.createElement("li");
    item.className = "empty-row";
    item.textContent = emptyLabel;
    container.append(item);
    return;
  }
  for (const point of points) {
    const item = document.createElement("li");
    const location = document.createElement("span");
    location.textContent = `SPX ${point.spot.toFixed(2)} · ${formatMinutes(point.minutesForward)}`;
    const value = document.createElement("strong");
    value.textContent = compactNumber(point.value, 3);
    item.append(location, value);
    container.append(item);
  }
}

function cancelSnapshotWork() {
  window.clearTimeout(app.timer);
  app.timer = null;
  app.requestGeneration += 1;
  if (app.requestController) app.requestController.abort();
  app.requestController = null;
}

function beginSnapshotRequest() {
  if (app.requestController) app.requestController.abort();
  const controller = new AbortController();
  const generation = ++app.requestGeneration;
  const abortTimer = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  app.requestController = controller;
  return { controller, generation, abortTimer };
}

function requestIsCurrent(generation, mode) {
  return app.requestGeneration === generation && app.mode === mode;
}

function clearDashboardState() {
  app.snapshot = null;
  app.expiry = "";
  updateFilters();
  setQualityChip("unavailable");
  dom.heatmapEmpty.hidden = false;
  clearCanvas();
  clearLadder();
  renderExtrema(null);
  updateLegend(null);
  dom.schemaVersion.textContent = "schema —";
  dom.signConvention.textContent = "dealer position sign unknown";
}

function renderLoadingState() {
  clearDashboardState();
  const replay = isReplayView();
  setStatusPill("unknown", replay ? "Loading replay" : "正在连接");
  dom.refreshState.textContent = replay ? "正在读取并校验冻结快照" : "等待首个快照";
  dom.summaryStatus.textContent = "—";
  dom.summaryReasons.textContent = replay ? "校验 replay / cutoff / lookahead 契约" : "尚未加载";
  dom.summaryFreshness.textContent = replay ? "Frozen · Not live" : "—";
  dom.summaryAsOf.textContent = replay ? `as of ${REPLAY.asOfLabel}` : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "可用合约 —";
  dom.summaryExpiries.textContent = "—";
  dom.summaryUnderlier.textContent = "SPX —";
  dom.surfaceTitle.textContent = "Spot × Time surface";
  dom.surfaceSubtitle.textContent = replay ? "等待冻结历史回放" : "等待生产快照";
  setNotice(replay ? "正在读取历史回放；不会显示实时快照或缓存值。" : "");
  updateModeChrome();
}

function renderFetchFailure(error, mode) {
  clearDashboardState();
  const replay = mode === "replay";
  const reason = error instanceof Error ? error.message : `${mode}_snapshot_fetch_failed`;
  setStatusPill("unavailable", replay ? "Replay unavailable" : null);
  dom.refreshState.textContent = replay ? "历史回放读取失败；未自动重试" : "读取失败；5 秒后重试";
  dom.summaryStatus.textContent = "Unavailable";
  dom.summaryReasons.textContent = reason;
  dom.summaryFreshness.textContent = replay ? "Frozen · Not live" : "—";
  dom.summaryAsOf.textContent = replay ? `as of ${REPLAY.asOfLabel}` : "as of —";
  dom.summaryCoverage.textContent = "—";
  dom.summaryContracts.textContent = "可用合约 —";
  dom.summaryExpiries.textContent = "—";
  dom.summaryUnderlier.textContent = "SPX —";
  dom.surfaceTitle.textContent = replay ? "Replay unavailable" : "Spot × Time surface";
  dom.surfaceSubtitle.textContent = replay ? `HISTORICAL REPLAY · Frozen · Not live · as of ${REPLAY.asOfLabel}` : "等待生产快照";
  setNotice(
    replay
      ? "无法读取或校验历史回放；图表已清空，不会回退到生产快照、fixture 或缓存值。"
      : "无法读取生产快照；图表已清空，页面不会显示 fixture 或上一次缓存值。",
    true,
  );
  updateModeChrome();
}

async function refreshSnapshot() {
  if (app.mode !== "live") return;
  window.clearTimeout(app.timer);
  app.timer = null;
  const { controller, generation, abortTimer } = beginSnapshotRequest();
  try {
    const response = await fetch(SNAPSHOT_URL, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`snapshot_http_${response.status}`);
    const payload = await response.json();
    const snapshot = normalizeSnapshot(payload);
    if (!requestIsCurrent(generation, "live")) return;
    app.snapshot = snapshot;
    render();
  } catch (error) {
    if (!requestIsCurrent(generation, "live")) return;
    renderFetchFailure(error, "live");
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
    if (requestIsCurrent(generation, "live")) {
      app.timer = window.setTimeout(refreshSnapshot, POLL_INTERVAL_MS);
    }
  }
}

async function loadReplaySnapshot() {
  if (app.mode !== "replay") return;
  const { controller, generation, abortTimer } = beginSnapshotRequest();
  try {
    const response = await fetch(REPLAY.url, {
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`replay_http_${response.status}`);
    const payload = await response.json();
    const snapshot = await normalizeReplaySnapshot(payload);
    if (!requestIsCurrent(generation, "replay")) return;
    app.snapshot = snapshot;
    render();
  } catch (error) {
    if (!requestIsCurrent(generation, "replay")) return;
    renderFetchFailure(error, "replay");
  } finally {
    window.clearTimeout(abortTimer);
    if (app.requestController === controller) app.requestController = null;
  }
}

function setViewMode(mode, { syncQuery = true } = {}) {
  if (!["live", "replay"].includes(mode)) return;
  cancelSnapshotWork();
  app.mode = mode;
  if (syncQuery) updateModeQuery();
  renderLoadingState();
  if (mode === "replay") loadReplaySnapshot();
  else refreshSnapshot();
}

dom.modeFilter.addEventListener("change", () => {
  setViewMode(dom.modeFilter.value === REPLAY.view ? "replay" : "live");
});
dom.expiryFilter.addEventListener("change", () => {
  app.expiry = dom.expiryFilter.value;
  renderSummary();
  renderVisuals();
});
dom.weightingFilter.addEventListener("change", () => {
  app.weighting = dom.weightingFilter.value;
  renderSummary();
  renderVisuals();
});
dom.metricFilter.addEventListener("change", () => {
  app.metric = dom.metricFilter.value;
  renderSummary();
  renderVisuals();
});
dom.heatmap.addEventListener("pointermove", (event) => {
  if (app.chartHit) tooltipForHit(app.chartHit, event);
});
dom.heatmap.addEventListener("pointerleave", () => {
  dom.heatmapTooltip.hidden = true;
});

if ("ResizeObserver" in window) {
  const resizeObserver = new ResizeObserver(() => {
    if (app.snapshot) window.requestAnimationFrame(renderVisuals);
  });
  resizeObserver.observe(dom.heatmapStage);
} else {
  window.addEventListener("resize", () => {
    if (app.snapshot) window.requestAnimationFrame(renderVisuals);
  });
}

setViewMode(app.mode, { syncQuery: false });
