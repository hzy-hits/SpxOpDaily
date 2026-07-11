CREATE OR REPLACE VIEW research_strategy_outcome_v1 AS
WITH eligible_decisions AS (
    SELECT *
    FROM _research_source_decisions_v1
    WHERE decision_id IS NOT NULL
      AND available_at IS NOT NULL
      AND decision_at IS NOT NULL
      AND available_at <= decision_at
),
leg_summary AS (
    SELECT
        l.decision_id AS decision_id,
        COUNT(*) AS leg_count,
        FIRST(l.side ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS leg_side,
        FIRST(instrument_id ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS instrument_id,
        FIRST(expiry ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS expiry,
        FIRST(strike ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS strike,
        FIRST(bid ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_bid,
        FIRST(ask ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_ask,
        FIRST(mark ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_mark,
        FIRST(delta ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_delta,
        FIRST(gamma ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_gamma,
        FIRST(theta ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_theta,
        FIRST(vega ORDER BY leg_index NULLS LAST, leg_id NULLS LAST) AS entry_vega
    FROM _research_source_decision_legs_v1 l
    JOIN eligible_decisions d ON d.decision_id = l.decision_id
    WHERE l.available_at IS NOT NULL
      AND l.available_at <= d.decision_at
    GROUP BY l.decision_id
),
delivery_summary AS (
    SELECT
        decision_id,
        COUNT(*) AS delivery_attempts,
        BOOL_OR(LOWER(COALESCE(status, '')) IN ('sent', 'delivered', 'success')) AS delivered,
        BOOL_OR(LOWER(COALESCE(status, '')) IN ('vetoed', 'blocked')) AS delivery_vetoed,
        BOOL_OR(
            LOWER(COALESCE(provider, '')) = 'deepseek'
            AND LOWER(COALESCE(status, '')) IN ('vetoed', 'blocked')
        ) AS deepseek_vetoed,
        STRING_AGG(
            DISTINCT reason_code,
            ',' ORDER BY reason_code
        ) FILTER (WHERE reason_code IS NOT NULL) AS delivery_reasons,
        MAX(sent_at) AS sent_at,
        STRING_AGG(DISTINCT channel, ',' ORDER BY channel) AS delivery_channels
    FROM _research_source_alert_deliveries_v1
    GROUP BY decision_id
),
decision_context AS (
    SELECT
        d.*,
        e.event_type,
        e.phase AS event_phase,
        e.direction AS event_direction,
        e.source_at AS event_source_at,
        e.available_at AS event_available_at,
        f.source_at AS feature_source_at,
        f.available_at AS feature_available_at,
        f.net_gamma,
        f.gamma_regime AS feature_gamma_regime,
        f.charm,
        f.vanna,
        f.color,
        f.speed,
        f.call_wall,
        f.put_wall,
        f.gamma_flip,
        f.iv,
        f.skew,
        f.payload_json AS feature_payload_json
    FROM eligible_decisions d
    LEFT JOIN LATERAL (
        SELECT e.*
        FROM _research_source_events_v1 e
        WHERE e.event_key = d.event_key
          AND e.available_at IS NOT NULL
          AND e.available_at <= d.decision_at
        ORDER BY e.available_at DESC, e.source_at DESC
        LIMIT 1
    ) e ON TRUE
    LEFT JOIN LATERAL (
        SELECT f.*
        FROM _research_source_feature_snapshots_v1 f
        WHERE f.available_at IS NOT NULL
          AND f.available_at <= d.decision_at
          AND (
              f.feature_snapshot_id = d.feature_snapshot_id
              OR f.decision_id = d.decision_id
              OR (f.event_key IS NOT NULL AND f.event_key = d.event_key)
          )
        ORDER BY f.available_at DESC, f.source_at DESC
        LIMIT 1
    ) f ON TRUE
),
joined AS (
    SELECT
        COALESCE(d.session_date, CAST(d.decision_at AS DATE)) AS session_date,
        d.decision_id,
        d.event_key,
        d.feature_snapshot_id,
        d.strategy_name,
        d.strategy_version,
        CASE UPPER(COALESCE(d.side, l.leg_side, ''))
            WHEN 'C' THEN 'CALL'
            WHEN 'CALL' THEN 'CALL'
            WHEN 'P' THEN 'PUT'
            WHEN 'PUT' THEN 'PUT'
            ELSE 'UNKNOWN'
        END AS option_side,
        d.action AS decision_action,
        d.status AS decision_status,
        d.reason_code AS decision_reason,
        COALESCE(d.llm_provider, CASE WHEN ds.deepseek_vetoed THEN 'deepseek' END) AS llm_provider,
        d.llm_decision,
        COALESCE(
            JSON_EXTRACT_STRING(d.payload_json, '$.record_kind'),
            'alert_decision'
        ) AS record_kind,
        CASE
            WHEN COALESCE(ds.delivery_vetoed, FALSE) THEN TRUE
            WHEN LOWER(COALESCE(d.status, '')) IN ('vetoed', 'veto', 'rejected', 'blocked') THEN TRUE
            WHEN LOWER(COALESCE(d.action, '')) IN ('vetoed', 'veto', 'reject', 'block') THEN TRUE
            WHEN LOWER(COALESCE(d.llm_decision, '')) IN ('vetoed', 'veto', 'reject', 'block') THEN TRUE
            WHEN LOWER(COALESCE(d.reason_code, '')) LIKE '%veto%' THEN TRUE
            ELSE FALSE
        END AS vetoed,
        CASE
            WHEN LOWER(COALESCE(d.status, '')) = 'selected'
             AND LOWER(COALESCE(d.action, '')) = 'notify'
             AND COALESCE(
                 JSON_EXTRACT_STRING(d.payload_json, '$.record_kind'),
                 'alert_decision'
             ) <> 'evaluation_context' THEN TRUE
            WHEN LOWER(COALESCE(d.status, '')) IN (
                'triggered', 'accepted', 'candidate', 'alerted', 'sent', 'delivered'
            ) THEN TRUE
            WHEN LOWER(COALESCE(d.action, '')) IN ('trigger', 'alert', 'send', 'enter') THEN TRUE
            ELSE FALSE
        END AS triggered,
        d.source_at AS decision_source_at,
        d.received_at AS decision_received_at,
        d.available_at AS decision_available_at,
        d.decision_at,
        TRUE AS anti_lookahead_valid,
        d.event_type,
        d.event_phase,
        d.event_direction,
        d.event_source_at,
        d.event_available_at,
        d.feature_source_at,
        d.feature_available_at,
        COALESCE(
            d.net_gamma,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.net_gamma') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.signed_gex') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.structure.net_gex') AS DOUBLE)
        ) AS net_gamma,
        COALESCE(d.feature_gamma_regime, d.gamma_regime, 'unknown') AS gamma_regime,
        COALESCE(
            d.charm,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.charm') AS DOUBLE)
        ) AS charm,
        COALESCE(
            d.vanna,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.vanna') AS DOUBLE)
        ) AS vanna,
        COALESCE(
            d.color,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.color') AS DOUBLE)
        ) AS color,
        COALESCE(
            d.speed,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.speed') AS DOUBLE)
        ) AS speed,
        COALESCE(
            d.call_wall,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.call_wall') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.structure.call_wall') AS DOUBLE)
        ) AS call_wall,
        COALESCE(
            d.put_wall,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.put_wall') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.structure.put_wall') AS DOUBLE)
        ) AS put_wall,
        COALESCE(
            d.gamma_flip,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.gamma_flip') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.flip') AS DOUBLE),
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.structure.zero_gamma') AS DOUBLE)
        ) AS gamma_flip,
        COALESCE(
            d.iv,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.iv') AS DOUBLE)
        ) AS iv,
        COALESCE(
            d.skew,
            TRY_CAST(JSON_EXTRACT(d.feature_payload_json, '$.skew') AS DOUBLE)
        ) AS skew,
        COALESCE(l.leg_count, 0) AS leg_count,
        l.instrument_id,
        l.expiry,
        l.strike,
        l.entry_bid,
        l.entry_ask,
        l.entry_mark,
        l.entry_delta,
        l.entry_gamma,
        l.entry_theta,
        l.entry_vega,
        COALESCE(ds.delivery_attempts, 0) AS delivery_attempts,
        COALESCE(ds.delivered, FALSE) AS delivered,
        COALESCE(ds.delivery_vetoed, FALSE) AS delivery_vetoed,
        COALESCE(ds.deepseek_vetoed, FALSE) AS deepseek_vetoed,
        ds.delivery_reasons,
        ds.sent_at,
        ds.delivery_channels,
        o.outcome_id,
        o.horizon_minutes,
        o.status AS outcome_status,
        o.reason_code AS outcome_reason,
        o.target_at,
        o.sample_at,
        o.available_at AS outcome_available_at,
        o.start_spx,
        o.end_spx,
        o.return_bps,
        o.mfe_bps,
        o.mae_bps,
        o.path_high_return_bps,
        o.path_low_return_bps,
        o.entry_price AS simulated_entry_price,
        o.exit_price AS simulated_exit_price,
        o.option_pnl,
        o.option_return_bps,
        o.option_return_pct
    FROM decision_context d
    LEFT JOIN leg_summary l ON l.decision_id = d.decision_id
    LEFT JOIN delivery_summary ds ON ds.decision_id = d.decision_id
    LEFT JOIN _research_source_outcomes_v1 o
      ON o.decision_id = d.decision_id
      OR (
          o.decision_id IS NULL
          AND o.event_key IS NOT NULL
          AND o.event_key = d.event_key
      )
)
SELECT
    *,
    CASE option_side
        WHEN 'CALL' THEN return_bps
        WHEN 'PUT' THEN -return_bps
        ELSE NULL
    END AS directional_return_bps,
    CASE option_side
        WHEN 'CALL' THEN path_high_return_bps
        WHEN 'PUT' THEN -path_low_return_bps
        ELSE NULL
    END AS directional_mfe_bps,
    CASE option_side
        WHEN 'CALL' THEN path_low_return_bps
        WHEN 'PUT' THEN -path_high_return_bps
        ELSE NULL
    END AS directional_mae_bps
FROM joined;
