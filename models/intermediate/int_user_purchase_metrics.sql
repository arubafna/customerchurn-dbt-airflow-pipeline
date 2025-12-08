{{
    config(
        materialized='table',
        tags=['intermediate', 'purchase', 'rfm']
    )
}}

/*
    Intermediate model for user purchase behavior metrics.
    
    Purpose:
        - Calculate RFM (Recency, Frequency, Monetary) features
        - Time-windowed purchase metrics (30/60/90 days)
        - Purchase velocity and trends
    Source: {{ ref('stg_transactions') }}
    Grain: One row per user
*/

WITH reference_date AS (
    -- Use the last date in the dataset as reference
    SELECT dateadd(day, 1, MAX(PURCHASE_DATE)) AS REF_DATE
    FROM {{ ref('stg_transactions') }}
),

user_purchases AS (
    SELECT
        USER_ID,
        PURCHASE_TIMESTAMP,
        PURCHASE_AMOUNT,
        PURCHASE_DATE,
        ITEMS_COUNT,
        (SELECT REF_DATE FROM reference_date) AS REFERENCE_DATE
    FROM {{ ref('stg_transactions') }}
),

purchase_metrics AS (
    SELECT
        USER_ID,
        REFERENCE_DATE,
        
        -- RECENCY: Days since last purchase (from reference date)
        DATEDIFF(DAY, MAX(PURCHASE_DATE), REFERENCE_DATE) AS DAYS_SINCE_LAST_PURCHASE,
        
        -- FREQUENCY: Purchase counts by time window (from reference date)
        COUNT(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN 1 END) AS PURCHASE_COUNT_30D,
        COUNT(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -60, REFERENCE_DATE) THEN 1 END) AS PURCHASE_COUNT_60D,
        COUNT(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -90, REFERENCE_DATE) THEN 1 END) AS PURCHASE_COUNT_90D,
        COUNT(*) AS LIFETIME_PURCHASE_COUNT,
        
        -- MONETARY: Spend by time window (from reference date)
        SUM(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN PURCHASE_AMOUNT ELSE 0 END) AS TOTAL_SPEND_30D,
        SUM(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -60, REFERENCE_DATE) THEN PURCHASE_AMOUNT ELSE 0 END) AS TOTAL_SPEND_60D,
        SUM(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -90, REFERENCE_DATE) THEN PURCHASE_AMOUNT ELSE 0 END) AS TOTAL_SPEND_90D,
        SUM(PURCHASE_AMOUNT) AS LIFETIME_VALUE,
        
        -- Average order value
        AVG(PURCHASE_AMOUNT) AS AVG_ORDER_VALUE,
        
        -- Product diversity
        SUM(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ITEMS_COUNT ELSE 0 END) AS TOTAL_ITEMS_30D,
        AVG(CASE WHEN PURCHASE_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ITEMS_COUNT END) AS AVG_ITEMS_PER_ORDER,
        
        -- First and last purchase dates
        MIN(PURCHASE_DATE) AS FIRST_PURCHASE_DATE,
        MAX(PURCHASE_DATE) AS LAST_PURCHASE_DATE
        
    FROM user_purchases
    GROUP BY USER_ID, REFERENCE_DATE
),

derived_metrics AS (
    SELECT
        *,
        
        -- Purchase velocity (trend indicator)
        CASE 
            WHEN PURCHASE_COUNT_60D > 0 
            THEN PURCHASE_COUNT_30D::FLOAT / NULLIF(PURCHASE_COUNT_60D, 0)
            ELSE 0 
        END AS PURCHASE_VELOCITY,
        
        -- Recency bucket
        CASE
            WHEN DAYS_SINCE_LAST_PURCHASE <= 7 THEN 'very_recent'
            WHEN DAYS_SINCE_LAST_PURCHASE <= 14 THEN 'recent'
            WHEN DAYS_SINCE_LAST_PURCHASE <= 30 THEN 'moderate'
            ELSE 'at_risk'
        END AS RECENCY_BUCKET,
        
        -- Days as customer
        DATEDIFF(DAY, FIRST_PURCHASE_DATE, LAST_PURCHASE_DATE) AS DAYS_AS_CUSTOMER
        
    FROM purchase_metrics
)

SELECT * FROM derived_metrics