{{ config(materialized='table') }}

SELECT
    SUM(number_flights_matched) AS total_tracked_flights
FROM {{ ref('stg_punctuality_data') }}
WHERE year = (SELECT MAX(year) FROM {{ ref('stg_punctuality_data') }})
  AND month = (
      SELECT MAX(month)
      FROM {{ ref('stg_punctuality_data') }}
      WHERE year = (SELECT MAX(year) FROM {{ ref('stg_punctuality_data') }})
  )