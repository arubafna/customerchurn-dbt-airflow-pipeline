-- File: int_user_purchase_metrics.sql
-- Purpose: Detect if purchase behavior is accelerating or decelerating

WITH reference_date AS (
    SELECT MAX(PURCHASE_DATE) as ref_date
    FROM {{ ref('stg_transactions') }}
),

purchase_windows AS (
    SELECT
        t.USER_ID,
        r.ref_date,
        -- Recent period: Last 30 days (days 0-30 from reference)
        COUNT(CASE WHEN t.PURCHASE_DATE >= DATEADD('day', -30, r.ref_date) THEN 1 END) as purchase_count_recent_30d,
        -- Prior period: Previous 30 days (days 31-60 from reference)
        COUNT(CASE WHEN t.PURCHASE_DATE >= DATEADD('day', -60, r.ref_date) AND t.PURCHASE_DATE < DATEADD('day', -30, r.ref_date) THEN 1 END) as purchase_count_prior_30d
    FROM {{ ref('stg_transactions') }} t
    CROSS JOIN reference_date r
    WHERE t.PURCHASE_DATE >= DATEADD('day', -60, r.ref_date)
    GROUP BY t.USER_ID, r.ref_date
)

SELECT
    USER_ID,
    purchase_count_recent_30d,
    purchase_count_prior_30d,
    -- Calculate velocity: recent vs prior period
    CASE
    WHEN purchase_count_prior_30d = 0 THEN NULL  -- No baseline
    ELSE ROUND(purchase_count_recent_30d * 1.0 / NULLIF(purchase_count_prior_30d, 0),2)
    END as purchase_velocity,
    -- Categorical interpretation
    CASE
    WHEN purchase_count_prior_30d = 0 THEN 'insufficient_data'
    WHEN purchase_count_recent_30d * 1.0 / NULLIF(purchase_count_prior_30d, 0) > 1.5
    THEN 'accelerating'
    WHEN purchase_count_recent_30d * 1.0 / NULLIF(purchase_count_prior_30d, 0) >= 0.8
    THEN 'steady'
    ELSE 'decelerating'
    END as velocity_trend
FROM purchase_windows