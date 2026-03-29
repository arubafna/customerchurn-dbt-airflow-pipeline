{{
    config(
        materialized='table',
        tags=['marts', 'features', 'ml_ready']
    )
}}

/*
    Final mart: Customer retention features for ML models.
    
    Purpose:
        - Combine all features into single denormalized table
        - One row per user with all 27 features
        - ML-ready format with no NULLs
        - Includes derived risk indicators and segments
    
    Sources: {{ ref('int_user_purchase_metrics') }}, {{ ref('int_user_session_metrics') }}, {{ ref('stg_activity_log') }}
    Grain: One row per user
*/

WITH reference_date AS (
    SELECT MAX(ACTIVITY_DATE) AS REF_DATE
    FROM {{ ref('stg_activity_log') }}
),

-- Get all unique users
all_users AS (
    SELECT DISTINCT USER_ID, (SELECT REF_DATE FROM reference_date) AS FEATURE_SNAPSHOT_DATE
    FROM {{ ref('stg_activity_log') }}
    WHERE USER_ID IS NOT NULL
    
    UNION
    
    SELECT DISTINCT USER_ID, (SELECT REF_DATE FROM reference_date) AS FEATURE_SNAPSHOT_DATE
    FROM {{ ref('stg_transactions') }}
),

-- Combine all features
final_features AS (
    SELECT
        u.USER_ID,
        u.FEATURE_SNAPSHOT_DATE,
        
        -- RECENCY FEATURES (3)
        COALESCE(p.DAYS_SINCE_LAST_PURCHASE, 999) AS DAYS_SINCE_LAST_PURCHASE,
        COALESCE(s.DAYS_SINCE_LAST_LOGIN, 999) AS DAYS_SINCE_LAST_LOGIN,
        COALESCE(p.RECENCY_BUCKET, 'never_purchased') AS RECENCY_BUCKET,
        
        -- FREQUENCY FEATURES (4)
        COALESCE(p.PURCHASE_COUNT_30D, 0) AS PURCHASE_COUNT_30D,
        COALESCE(p.PURCHASE_COUNT_60D, 0) AS PURCHASE_COUNT_60D,
        COALESCE(p.PURCHASE_COUNT_90D, 0) AS PURCHASE_COUNT_90D,
        COALESCE(s.SESSION_COUNT_30D, 0) AS SESSION_COUNT_30D,
        
        -- MONETARY FEATURES (5)
        COALESCE(p.TOTAL_SPEND_30D, 0) AS TOTAL_SPEND_30D,
        COALESCE(p.TOTAL_SPEND_60D, 0) AS TOTAL_SPEND_60D,
        COALESCE(p.TOTAL_SPEND_90D, 0) AS TOTAL_SPEND_90D,
        COALESCE(p.AVG_ORDER_VALUE, 0) AS AVG_ORDER_VALUE,
        COALESCE(p.LIFETIME_VALUE, 0) AS LIFETIME_VALUE,
        
        -- ENGAGEMENT FEATURES (4)
        COALESCE(s.AVG_ACTIONS_PER_SESSION, 0) AS AVG_ACTIONS_PER_SESSION,
        COALESCE(s.AVG_SESSION_DURATION_SECONDS, 0) AS AVG_SESSION_DURATION_SECONDS,
        COALESCE(s.MAX_ACTIONS_PER_SESSION, 0) AS MAX_ACTIONS_PER_SESSION,
        COALESCE(s.ENGAGEMENT_SCORE, 0) AS ENGAGEMENT_SCORE,
        
        -- BEHAVIORAL FEATURES (7)
        COALESCE(s.SEARCH_COUNT_30D, 0) AS SEARCH_COUNT_30D,
        COALESCE(s.ERROR_COUNT_30D, 0) AS ERROR_COUNT_30D,
        COALESCE(s.ERROR_RATE, 0) AS ERROR_RATE,
        COALESCE(s.CART_ABANDONMENT_RATE, 0) AS CART_ABANDONMENT_RATE,
        COALESCE(s.PURCHASE_CONVERSION_RATE, 0) AS PURCHASE_CONVERSION_RATE,
        COALESCE(s.SEARCH_RATE, 0) AS SEARCH_RATE,
        COALESCE(p.PURCHASE_VELOCITY, 0) AS PURCHASE_VELOCITY,
        
        -- DEVICE PREFERENCE (1)
        COALESCE(s.PRIMARY_DEVICE, 'unknown') AS PRIMARY_DEVICE,
        
        -- DERIVED/RISK INDICATORS (3)
        CASE
            WHEN COALESCE(p.DAYS_SINCE_LAST_PURCHASE, 999) > 30 
                 AND COALESCE(s.SESSION_COUNT_30D, 0) < 10
            THEN TRUE
            WHEN COALESCE(s.CART_ABANDONMENT_RATE, 0) > 0.8
                 AND COALESCE(p.PURCHASE_COUNT_30D, 0) = 0
            THEN TRUE
            WHEN COALESCE(s.ERROR_RATE, 0) > 0.1
                 AND COALESCE(s.SESSION_COUNT_30D, 0) < 5
            THEN TRUE
            ELSE FALSE
        END AS IS_HIGH_CHURN_RISK,
        
        CASE
            WHEN COALESCE(s.SESSION_COUNT_30D, 0) >= 30 
                 AND COALESCE(p.PURCHASE_COUNT_30D, 0) >= 2
            THEN 'high'
            WHEN COALESCE(s.SESSION_COUNT_30D, 0) >= 15 
                 AND COALESCE(p.PURCHASE_COUNT_30D, 0) >= 1
            THEN 'medium'
            WHEN COALESCE(s.SESSION_COUNT_30D, 0) > 0
            THEN 'low'
            ELSE 'inactive'
        END AS ACTIVITY_LEVEL,
        
        CASE
            WHEN COALESCE(p.LIFETIME_VALUE, 0) > 3000 
                 AND COALESCE(p.PURCHASE_COUNT_90D, 0) >= 3
            THEN 'vip'
            WHEN COALESCE(p.LIFETIME_VALUE, 0) > 1500 
                 AND COALESCE(p.PURCHASE_COUNT_90D, 0) >= 2
            THEN 'loyal'
            WHEN COALESCE(p.PURCHASE_COUNT_90D, 0) >= 1
            THEN 'active'
            WHEN COALESCE(s.SESSION_COUNT_30D, 0) > 0
            THEN 'browser'
            ELSE 'dormant'
        END AS CUSTOMER_SEGMENT
        
    FROM all_users u
    LEFT JOIN {{ ref('int_user_purchase_metrics') }} p
        ON u.USER_ID = p.USER_ID
    LEFT JOIN {{ ref('int_user_session_metrics') }} s
        ON u.USER_ID = s.USER_ID
)

SELECT * FROM final_features