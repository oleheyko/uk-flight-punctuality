-- Table shows how average delay minutes change over the years
{{ config(materialized='table') }}
select
    year,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
from {{ ref('int_scheduled_flights') }}
where number_flights_matched > 0
group by year
order by year