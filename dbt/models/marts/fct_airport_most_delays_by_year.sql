-- Find the airport with the highest average delay by year
{{ config(materialized='view') }}

with delays_by_airport_year as (
    select
        reporting_airport,
        year,
        sum(number_flights_matched) as total_flights,
        sum(number_flights_cancelled) as total_cancelled_flights,
        round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
    from {{ ref('int_scheduled_flights') }}
    where number_flights_matched > 0
      and average_delay_mins is not null
      and not is_nan(average_delay_mins)
    group by reporting_airport, year
),

ranked_delays as (
    select
        reporting_airport,
        year,
        total_flights,
        total_cancelled_flights,
        avg_delay_mins,
        row_number() over (partition by year order by avg_delay_mins desc) as rank_in_year
    from delays_by_airport_year
)

select
    reporting_airport,
    year,
    total_flights,
    total_cancelled_flights,
    avg_delay_mins
from ranked_delays
where rank_in_year = 1
order by year