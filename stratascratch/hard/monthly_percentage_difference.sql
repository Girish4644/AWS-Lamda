-- Problem: Monthly Percentage Difference
-- Goal: Month-over-month percentage change in revenue.
-- Output:
--   year_month (YYYY-MM), revenue_diff_pct
-- Notes:
--   Starts from 2nd month; rounded to 2 decimals.

WITH monthly_revenue AS (
    SELECT
        TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') AS year_month,
        SUM(value) AS revenue
    FROM sf_transactions
    GROUP BY 1
),
revenue_with_prev AS (
    SELECT
        year_month,
        revenue,
        LAG(revenue) OVER (ORDER BY year_month) AS prev_month_revenue
    FROM monthly_revenue
)
SELECT
    year_month,
    ROUND(
        ((revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0)) * 100,
        2
    ) AS revenue_diff_pct
FROM revenue_with_prev
WHERE prev_month_revenue IS NOT NULL
ORDER BY year_month;
