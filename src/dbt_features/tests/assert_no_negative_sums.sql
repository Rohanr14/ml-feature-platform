-- Validate no negative transaction sums in daily features.
-- A negative sum would indicate a data quality issue upstream.

SELECT
    user_id,
    event_date,
    daily_txn_sum
FROM {{ ref('user_daily_features') }}
WHERE daily_txn_sum < 0
