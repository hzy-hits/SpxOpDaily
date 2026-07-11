CREATE OR REPLACE VIEW session_data_quality_v1 AS
WITH normalized AS (
    SELECT
        COALESCE(
            session_date,
            TRY_CAST(REGEXP_EXTRACT(source_file, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) AS DATE),
            CAST(verified_at AS DATE)
        ) AS session_date,
        COALESCE(
            provider,
            NULLIF(REGEXP_EXTRACT(source_file, 'provider=([^/]+)', 1), ''),
            'unknown'
        ) AS provider,
        COALESCE(
            dataset,
            NULLIF(REGEXP_EXTRACT(source_file, '(^|/)lake/([^/]+)', 2), ''),
            'unknown'
        ) AS dataset,
        * EXCLUDE (session_date, provider, dataset)
    FROM _research_source_session_manifests_v1
)
SELECT
    session_date,
    COALESCE(provider, 'unknown') AS provider,
    COALESCE(dataset, 'unknown') AS dataset,
    COUNT(*) AS partition_count,
    COUNT(*) FILTER (
        WHERE LOWER(COALESCE(status, '')) IN ('verified', 'complete', 'success', 'ok')
    ) AS verified_partition_count,
    SUM(COALESCE(row_count, 0)) AS row_count,
    MIN(min_source_at) AS min_source_at,
    MAX(max_source_at) AS max_source_at,
    MAX(max_gap_seconds) AS max_gap_seconds,
    CASE
        WHEN SUM(CASE WHEN stale_ratio IS NOT NULL THEN COALESCE(row_count, 1) ELSE 0 END) > 0
        THEN SUM(COALESCE(stale_ratio, 0) * COALESCE(row_count, 1))
             / SUM(CASE WHEN stale_ratio IS NOT NULL THEN COALESCE(row_count, 1) ELSE 0 END)
        ELSE NULL
    END AS weighted_stale_ratio,
    CASE
        WHEN SUM(CASE WHEN missing_ratio IS NOT NULL THEN COALESCE(row_count, 1) ELSE 0 END) > 0
        THEN SUM(COALESCE(missing_ratio, 0) * COALESCE(row_count, 1))
             / SUM(CASE WHEN missing_ratio IS NOT NULL THEN COALESCE(row_count, 1) ELSE 0 END)
        ELSE NULL
    END AS weighted_missing_ratio,
    MIN(verified_at) AS first_verified_at,
    MAX(verified_at) AS last_verified_at,
    BOOL_AND(
        LOWER(COALESCE(status, '')) IN ('verified', 'complete', 'success', 'ok')
    ) AND SUM(COALESCE(row_count, 0)) > 0 AS is_research_ready
FROM normalized
GROUP BY session_date, COALESCE(provider, 'unknown'), COALESCE(dataset, 'unknown');
