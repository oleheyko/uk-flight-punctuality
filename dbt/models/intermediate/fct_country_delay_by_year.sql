-- Country-level delay summary by year
{{ config(materialized='view') }}

select
    origin_destination_country,
    sum(number_flights_matched) as total_flights_matched,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins,
    round(avg(`0_to_15_mins_late_percent`), 2) as avg_0_to_15_mins_late_percent
from {{ ref('stg_punctuality_data') }}
where year = (select max(year) from {{ ref('int_scheduled_flights') }})
  and number_flights_matched > 0
group by origin_destination_country, year
order by avg_delay_mins desc
