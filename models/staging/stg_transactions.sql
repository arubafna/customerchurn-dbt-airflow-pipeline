{{
    config(
        materialized='view',
        tags=['staging', 'transactions']
    )
}}

/*
    Staging model for transaction data.
    
    Purpose:
        - Parse JSON transaction data
        - Extract relevant fields
        - Type cast for downstream processing
    
    Source: RAW.TRANSACTIONS (JSON/VARIANT)
    Grain: One row per transaction
*/

WITH source_data AS (
    SELECT
        -- Extract fields from JSON VARIANT column
        RAW_DATA:transaction_id::STRING AS TRANSACTION_ID,
        RAW_DATA:user_id::STRING AS USER_ID,
        RAW_DATA:purchase_amount::DECIMAL(10,2) AS PURCHASE_AMOUNT,
        RAW_DATA:timestamp::TIMESTAMP_NTZ AS PURCHASE_TIMESTAMP,
        DATE(RAW_DATA:timestamp::TIMESTAMP_NTZ) AS PURCHASE_DATE,
        RAW_DATA:items_count::INT AS ITEMS_COUNT,
        RAW_DATA:items AS ITEMS_ARRAY,
        _LOADED_AT AS LOADED_AT,
        _FILENAME AS SOURCE_FILENAME
    FROM {{ source('raw', 'transactions') }}
    WHERE RAW_DATA:transaction_id IS NOT NULL
      AND RAW_DATA:user_id IS NOT NULL
      AND RAW_DATA:purchase_amount IS NOT NULL
      AND RAW_DATA:timestamp IS NOT NULL
)

SELECT * FROM source_data