-- Implement recent airport delays table, which shows the average delay minutes for flights departing from each airport in the most recent month.
{{ config(materialized='table') }}
select
    reporting_airport,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
from {{ ref('int_scheduled_flights') }}
where year = (select max(year) from {{ ref('int_scheduled_flights') }})
  and month = (
      select max(month)
      from {{ ref('int_scheduled_flights') }}
      where year = (select max(year) from {{ ref('int_scheduled_flights') }})
  )
group by reporting_airport