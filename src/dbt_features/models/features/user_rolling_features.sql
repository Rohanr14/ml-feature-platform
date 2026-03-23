-- Rolling 7-day and 30-day features per user
-- Captures longer-term behavioral patterns

{{ config(materialized='table') }}

WITH daily AS (
    SELECT * FROM {{ ref('user_daily_features') }}
)

SELECT
    user_id,
    event_date,

    -- 7-day rolling
    SUM(daily_txn_count) OVER (
        PARTITION BY user_id ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_txn_count,

    AVG(daily_txn_avg) OVER (
        PARTITION BY user_id ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_amount,

    -- 30-day rolling
    SUM(daily_txn_count) OVER (
        PARTITION BY user_id ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_txn_count,

    AVG(daily_txn_avg) OVER (
        PARTITION BY user_id ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg_amount

FROM daily
