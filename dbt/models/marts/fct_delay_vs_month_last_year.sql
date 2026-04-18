-- Table shows how average delay minutes change across months in the last year
{{ config(materialized='table') }}

SELECT
    month,
    CASE month
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    ROUND(
        SUM(number_flights_matched * average_delay_mins) / NULLIF(SUM(number_flights_matched), 0),
        2
    ) AS avg_delay_mins
FROM {{ ref('int_scheduled_flights') }}
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
  AND number_flights_matched > 0
GROUP BY month
ORDER BY month