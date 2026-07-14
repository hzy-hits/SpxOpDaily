CREATE OR REPLACE VIEW research_quotes_v1 AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                provider,
                session_date,
                received_at,
                source_at,
                available_at,
                quote_time,
                trade_time,
                last_update_at,
                instrument_id,
                symbol,
                instrument_type,
                provider_symbol,
                exchange,
                currency,
                expiry,
                strike,
                "right",
                multiplier,
                underlier,
                trading_class,
                bid,
                ask,
                last,
                mark,
                close,
                bid_size,
                ask_size,
                last_size,
                volume,
                open_interest,
                mid,
                spread,
                spread_bps,
                effective_price,
                quality,
                source_latency_ms,
                market_data_type,
                market_session,
                regular_source_at,
                extended_source_at,
                implied_vol,
                delta,
                gamma,
                theta,
                vega,
                rho,
                greeks_underlier_price,
                greeks_model,
                sampling_mode,
                sampling_group,
                error
            ORDER BY
                TRY_CAST(REGEXP_EXTRACT(schema_version, '([0-9]+)$', 1) AS INTEGER)
                    DESC NULLS LAST,
                (
                    CAST(bid IS NOT NULL AS INTEGER)
                    + CAST(ask IS NOT NULL AS INTEGER)
                    + CAST(last IS NOT NULL AS INTEGER)
                    + CAST(mark IS NOT NULL AS INTEGER)
                    + CAST(implied_vol IS NOT NULL AS INTEGER)
                    + CAST(delta IS NOT NULL AS INTEGER)
                    + CAST(gamma IS NOT NULL AS INTEGER)
                    + CAST(theta IS NOT NULL AS INTEGER)
                    + CAST(vega IS NOT NULL AS INTEGER)
                ) DESC,
                compacted_at DESC NULLS LAST,
                writer_version DESC NULLS LAST,
                source_file DESC NULLS LAST
        ) AS _quote_row
    FROM _research_source_quotes_v1
)
SELECT * EXCLUDE (_quote_row)
FROM ranked
WHERE _quote_row = 1;
