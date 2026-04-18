{{ config(materialized='table') }}

SELECT
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
    END || ' ' || CAST(year AS STRING) AS recent_published_date
FROM {{ ref('stg_punctuality_data') }}
WHERE year = (SELECT MAX(year) FROM {{ ref('stg_punctuality_data') }})
  AND month = (
      SELECT MAX(month)
      FROM {{ ref('stg_punctuality_data') }}
      WHERE year = (SELECT MAX(year) FROM {{ ref('stg_punctuality_data') }})
  )
LIMIT 1