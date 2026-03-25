-- Problem: Customer Tracking
-- Goal: Total active hours per customer.
-- Session rule: session starts at state=1 and ends at next state=0.

WITH ordered_events AS (
    SELECT
        cust_id,
        state,
        "timestamp" AS event_time,
        LEAD(state) OVER (
            PARTITION BY cust_id
            ORDER BY "timestamp"
        ) AS next_state,
        LEAD("timestamp") OVER (
            PARTITION BY cust_id
            ORDER BY "timestamp"
        ) AS next_event_time
    FROM cust_tracking
),
session_durations AS (
    SELECT
        cust_id,
        EXTRACT(EPOCH FROM (next_event_time - event_time)) / 3600.0 AS session_hours
    FROM ordered_events
    WHERE state = 1
      AND next_state = 0
      AND next_event_time IS NOT NULL
)
SELECT
    cust_id,
    ROUND(SUM(session_hours)::numeric, 2) AS total_hours
FROM session_durations
GROUP BY cust_id
ORDER BY cust_id;
