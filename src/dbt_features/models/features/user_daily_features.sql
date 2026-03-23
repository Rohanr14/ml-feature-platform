-- Batch features: daily aggregations per user
-- Fed into Feast as an offline feature source

{{ config(materialized='table') }}

SELECT
    user_id,
    event_date,

    -- Volume
    COUNT(*) AS daily_txn_count,
    SUM(amount) AS daily_txn_sum,
    AVG(amount) AS daily_txn_avg,
    MAX(amount) AS daily_txn_max,

    -- Diversity
    COUNT(DISTINCT category) AS daily_unique_categories,
    COUNT(DISTINCT merchant_id) AS daily_unique_merchants,

    -- Device
    COUNT(DISTINCT device_type) AS daily_unique_devices,

    -- Temporal
    MIN(event_hour) AS first_txn_hour,
    MAX(event_hour) AS last_txn_hour,

    -- Anomaly (label for training)
    MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS has_anomaly

FROM {{ ref('stg_transactions') }}
GROUP BY user_id, event_date
