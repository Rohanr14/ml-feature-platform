-- Staging: clean raw transactions from Delta Lake
-- Deduplicates, casts types, adds derived time columns
--
-- NOTE: The raw.transactions source is loaded via a pre-hook that
-- reads from Delta Lake using DuckDB's delta extension.
-- See macros/load_delta_source.sql

{{ config(
    materialized='view',
    pre_hook="CREATE OR REPLACE VIEW raw_transactions AS SELECT * FROM delta_scan('s3://ml-feature-platform/delta/raw-transactions/')"
) }}

SELECT
    transaction_id,
    user_id,
    CAST(amount AS DECIMAL(10, 2)) AS amount,
    currency,
    category,
    merchant_id,
    CAST("timestamp" AS TIMESTAMP) AS event_timestamp,
    is_anomaly,
    session_id,
    device_type,
    location_country,
    -- Derived time features
    CAST("timestamp" AS DATE) AS event_date,
    EXTRACT(HOUR FROM CAST("timestamp" AS TIMESTAMP)) AS event_hour,
    EXTRACT(DOW FROM CAST("timestamp" AS TIMESTAMP)) AS event_dow
FROM raw_transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY "timestamp") = 1
