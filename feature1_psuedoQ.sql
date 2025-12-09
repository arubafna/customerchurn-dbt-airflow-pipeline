-- File: int_user_purchase_metrics.sql
-- Purpose: Calculate recency metric for RFM analysis

WITH purchase_dates AS (
    SELECT 
        user_id,
        MAX(purchase_date) as last_purchase_date
    FROM {{ ref('stg_transactions') }}
    GROUP BY user_id
),

reference_date AS (
    -- Use data's max date + 1, not CURRENT_DATE (reproducibility)
    SELECT dateadd(day, 1, MAX(PURCHASE_DATE)) as ref_date
    FROM {{ ref('stg_transactions') }}
)

SELECT
    p.user_id,
    -- Calculate days since last purchase
    DATEDIFF('day', p.last_purchase_date, r.ref_date) as days_since_last_purchase,
    -- Categorize into buckets for segmentation
    CASE
        WHEN DATEDIFF('day', p.last_purchase_date, r.ref_date) <= 7 
            THEN 'very_recent'
        WHEN DATEDIFF('day', p.last_purchase_date, r.ref_date) <= 30 
            THEN 'recent'
        WHEN DATEDIFF('day', p.last_purchase_date, r.ref_date) <= 90 
            THEN 'moderate'
        ELSE 'at_risk'
    END as recency_bucket

FROM purchase_dates p
CROSS JOIN reference_date r