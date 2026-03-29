-- File: int_user_session_metrics.sql
-- Purpose: Calculate cart abandonment as friction indicator

WITH cart_adds AS (
    -- Users who added items to cart
    SELECT user_id, session_id, event_timestamp as cart_add_time
    FROM {{ ref('stg_activity_log') }}
    WHERE action_type = 'cart_add'
),

purchases AS (
    -- Actual purchases
    SELECT user_id, purchase_date as purchase_time
    FROM {{ ref('stg_transactions') }}
),

cart_with_purchase AS (
    -- Link cart adds to purchases within 5-minute window
    SELECT  c.user_id, c.session_id, c.cart_add_time,
    CASE 
        WHEN p.purchase_time IS NOT NULL 
        AND p.purchase_time BETWEEN c.cart_add_time 
        AND DATEADD('minute', 5, c.cart_add_time)
        THEN 1 
        ELSE 0 
    END as converted
    FROM cart_adds c
    LEFT JOIN purchases p
    ON c.user_id = p.user_id
)

SELECT
    user_id,
    -- Count total cart adds
    COUNT(*) as cart_add_count,
    -- Count conversions
    SUM(converted) as cart_converted_count,
    -- Calculate abandonment rate (handle zero division)
    ROUND((COUNT(*) - SUM(converted)) * 100.0 / NULLIF(COUNT(*), 0),2) as cart_abandonment_rate
FROM cart_with_purchase
GROUP BY user_id