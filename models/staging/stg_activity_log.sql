{{
    config(
        materialized='view',
        tags=['staging', 'activity']
    )
}}

/*
    Staging model for customer activity log data.
    
    Purpose:
        - Clean and standardize raw activity events
        - Remove invalid records
        - Prepare for downstream feature engineering
    
    Source: RAW.ACTIVITY_LOG
    Grain: One row per activity event
*/

WITH source_data AS (
    SELECT
        LOG_ID,
        SESSION_ID,
        USER_ID,
        UPPER(TRIM(ACTION_TYPE)) AS ACTION_TYPE,
        LOWER(TRIM(DEVICE)) AS DEVICE,
        TIMESTAMP AS ACTIVITY_TIMESTAMP,
        DATE(TIMESTAMP) AS ACTIVITY_DATE,
        EXTRACT(HOUR FROM TIMESTAMP) AS ACTIVITY_HOUR,
        _LOADED_AT AS LOADED_AT,
        _FILENAME AS SOURCE_FILENAME 
    FROM {{ source('raw', 'activity_log') }}
    WHERE 
    LOG_ID IS NOT NULL
    AND TIMESTAMP IS NOT NULL
    AND ACTION_TYPE IS NOT NULL
    AND TIMESTAMP <= CURRENT_TIMESTAMP()
)

SELECT * FROM source_data