CREATE OR REPLACE VIEW put_call_bias_audit_v1 AS
SELECT
    session_date,
    strategy_name,
    strategy_version,
    option_side,
    COALESCE(gamma_regime, 'unknown') AS gamma_regime,
    horizon_minutes,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) AS decision_count,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) FILTER (WHERE triggered) AS triggered_count,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) FILTER (WHERE vetoed) AS vetoed_count,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) FILTER (
        WHERE deepseek_vetoed
    ) AS deepseek_vetoed_count,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) FILTER (WHERE delivered) AS delivered_count,
    COUNT(DISTINCT COALESCE(event_key, decision_id)) FILTER (
        WHERE outcome_status = 'complete'
    ) AS complete_outcome_count,
    AVG(directional_return_bps) FILTER (WHERE outcome_status = 'complete') AS avg_directional_return_bps,
    MEDIAN(directional_return_bps) FILTER (
        WHERE outcome_status = 'complete'
    ) AS median_directional_return_bps,
    AVG(directional_mfe_bps) FILTER (WHERE outcome_status = 'complete') AS avg_directional_mfe_bps,
    AVG(directional_mae_bps) FILTER (WHERE outcome_status = 'complete') AS avg_directional_mae_bps,
    AVG(
        CASE
            WHEN outcome_status = 'complete' AND directional_return_bps IS NOT NULL
            THEN CASE WHEN directional_return_bps > 0 THEN 1.0 ELSE 0.0 END
            ELSE NULL
        END
    ) AS directional_win_rate,
    AVG(option_pnl) FILTER (WHERE outcome_status = 'complete') AS avg_option_pnl,
    SUM(option_pnl) FILTER (WHERE outcome_status = 'complete') AS total_option_pnl
FROM research_strategy_outcome_v1
GROUP BY
    session_date,
    strategy_name,
    strategy_version,
    option_side,
    COALESCE(gamma_regime, 'unknown'),
    horizon_minutes;
