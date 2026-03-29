{{
    config(
        materialized='table',
        tags=['intermediate', 'session', 'engagement']
    )
}}

/*
    Intermediate model for user session and engagement metrics.
    
    Purpose:
        - Calculate session-level engagement features
        - Link sessions to purchases
        - Calculate behavioral metrics (cart abandonment, error rates, etc.)
    
    Sources: {{ ref('stg_activity_log') }}, {{ ref('stg_transactions') }}
    Grain: One row per user
*/

WITH reference_date AS (
    SELECT MAX(ACTIVITY_DATE) AS REF_DATE
    FROM {{ ref('stg_activity_log') }}
),

user_activities AS (
    SELECT
        USER_ID,
        SESSION_ID,
        ACTION_TYPE,
        ACTIVITY_TIMESTAMP,
        ACTIVITY_DATE,
        DEVICE,
        (SELECT REF_DATE FROM reference_date) AS REFERENCE_DATE
    FROM {{ ref('stg_activity_log') }}
    WHERE USER_ID IS NOT NULL
),

user_transactions AS (
    SELECT
        USER_ID,
        TRANSACTION_ID,
        PURCHASE_TIMESTAMP,
        PURCHASE_DATE,
        PURCHASE_AMOUNT
    FROM {{ ref('stg_transactions') }}
),

session_aggregates AS (
    SELECT
        USER_ID,
        SESSION_ID,
        REFERENCE_DATE,
        COUNT(*) AS ACTIONS_IN_SESSION,
        DATEDIFF(SECOND, MIN(ACTIVITY_TIMESTAMP), MAX(ACTIVITY_TIMESTAMP)) AS SESSION_DURATION_SECONDS,
        MIN(ACTIVITY_TIMESTAMP) AS SESSION_START,
        MAX(ACTIVITY_TIMESTAMP) AS SESSION_END,
        MAX(ACTIVITY_DATE) AS SESSION_DATE,
        MODE(DEVICE) AS SESSION_DEVICE,
        -- Action counts per session
        SUM(CASE WHEN ACTION_TYPE = 'VIEW' THEN 1 ELSE 0 END) AS VIEW_COUNT,
        SUM(CASE WHEN ACTION_TYPE = 'SEARCH' THEN 1 ELSE 0 END) AS SEARCH_COUNT,
        SUM(CASE WHEN ACTION_TYPE = 'ADD_TO_CART' THEN 1 ELSE 0 END) AS CART_ADD_COUNT,
        SUM(CASE WHEN ACTION_TYPE = 'LOGIN' THEN 1 ELSE 0 END) AS LOGIN_COUNT,
        SUM(CASE WHEN ACTION_TYPE = 'LOGOUT' THEN 1 ELSE 0 END) AS LOGOUT_COUNT,
        SUM(CASE WHEN ACTION_TYPE = 'ERROR' THEN 1 ELSE 0 END) AS ERROR_COUNT
    FROM user_activities
    GROUP BY USER_ID, SESSION_ID, REFERENCE_DATE
),

-- Link sessions to purchases
sessions_with_purchases AS (
    SELECT
        s.*,
        t.TRANSACTION_ID,
        t.PURCHASE_AMOUNT,
        CASE 
            WHEN t.TRANSACTION_ID IS NOT NULL THEN 1 
            ELSE 0 
        END AS HAS_PURCHASE
    FROM session_aggregates s
    LEFT JOIN user_transactions t
        ON s.USER_ID = t.USER_ID
        AND t.PURCHASE_TIMESTAMP >= s.SESSION_START
        AND t.PURCHASE_TIMESTAMP <= DATEADD(MINUTE, 5, s.SESSION_END)
),

user_session_metrics AS (
    SELECT
        USER_ID,
        REFERENCE_DATE,
        -- RECENCY
        DATEDIFF(DAY, MAX(SESSION_DATE), REFERENCE_DATE) AS DAYS_SINCE_LAST_LOGIN,
        -- FREQUENCY: Session counts by time window
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN 1 END) AS SESSION_COUNT_30D,
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -60, REFERENCE_DATE) THEN 1 END) AS SESSION_COUNT_60D,
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -90, REFERENCE_DATE) THEN 1 END) AS SESSION_COUNT_90D,
        -- ENGAGEMENT: Actions and duration
        AVG(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ACTIONS_IN_SESSION END) AS AVG_ACTIONS_PER_SESSION,
        AVG(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN SESSION_DURATION_SECONDS END) AS AVG_SESSION_DURATION_SECONDS,
        MAX(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ACTIONS_IN_SESSION END) AS MAX_ACTIONS_PER_SESSION,
        -- BEHAVIORAL: Action counts (30 days)
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN SEARCH_COUNT ELSE 0 END) AS SEARCH_COUNT_30D,
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ERROR_COUNT ELSE 0 END) AS ERROR_COUNT_30D,
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN CART_ADD_COUNT ELSE 0 END) AS CART_ADD_COUNT_30D,
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN VIEW_COUNT ELSE 0 END) AS VIEW_COUNT_30D,
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN LOGIN_COUNT ELSE 0 END) AS LOGIN_COUNT_30D,
        -- Purchase-session linkage (30 days)
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) AND HAS_PURCHASE = 1 THEN 1 END) AS PURCHASE_SESSION_COUNT_30D,
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) AND CART_ADD_COUNT > 0 THEN 1 END) AS SESSIONS_WITH_CART_ADD_30D,
        COUNT(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) AND CART_ADD_COUNT > 0 AND HAS_PURCHASE = 0 THEN 1 END) AS ABANDONED_CART_SESSIONS_30D,
        -- Total actions
        SUM(CASE WHEN SESSION_DATE >= DATEADD(DAY, -30, REFERENCE_DATE) THEN ACTIONS_IN_SESSION ELSE 0 END) AS TOTAL_ACTIONS_30D,
        -- DEVICE
        MODE(SESSION_DEVICE) AS PRIMARY_DEVICE
    FROM sessions_with_purchases
    GROUP BY USER_ID, REFERENCE_DATE
),

derived_metrics AS (
    SELECT
        *,
        -- Cart abandonment rate
        CASE 
            WHEN SESSIONS_WITH_CART_ADD_30D > 0 
            THEN ABANDONED_CART_SESSIONS_30D::FLOAT / NULLIF(SESSIONS_WITH_CART_ADD_30D, 0)
            ELSE NULL
        END AS CART_ABANDONMENT_RATE,
        -- Purchase conversion rate
        CASE 
            WHEN SESSION_COUNT_30D > 0 
            THEN PURCHASE_SESSION_COUNT_30D::FLOAT / NULLIF(SESSION_COUNT_30D, 0)
            ELSE 0
        END AS PURCHASE_CONVERSION_RATE,
        -- Error rate
        CASE 
            WHEN TOTAL_ACTIONS_30D > 0 
            THEN ERROR_COUNT_30D::FLOAT / NULLIF(TOTAL_ACTIONS_30D, 0)
            ELSE 0 
        END AS ERROR_RATE,
        -- Search rate
        CASE 
            WHEN TOTAL_ACTIONS_30D > 0 
            THEN SEARCH_COUNT_30D::FLOAT / NULLIF(TOTAL_ACTIONS_30D, 0)
            ELSE 0 
        END AS SEARCH_RATE,
        -- Engagement score
        (SESSION_COUNT_30D * 0.3) + 
        (COALESCE(AVG_ACTIONS_PER_SESSION, 0) * 0.2) + 
        (SEARCH_COUNT_30D * 0.1) +
        (VIEW_COUNT_30D * 0.1) +
        (COALESCE(PURCHASE_CONVERSION_RATE, 0) * 100 * 0.3) AS ENGAGEMENT_SCORE
    FROM user_session_metrics
)

SELECT * FROM derived_metrics