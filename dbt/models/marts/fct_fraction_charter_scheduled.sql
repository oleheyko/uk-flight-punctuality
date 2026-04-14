-- Table showing fractions of charter/scheduled flights of total flights by year
{{ config(materialized='view') }}

with yearly_counts as (
    select
        year,
        sum(case when scheduled_charter = 'C' then number_flights_matched else 0 end) as total_charter_flights,
        sum(case when scheduled_charter = 'S' then number_flights_matched else 0 end) as total_scheduled_flights,
        sum(number_flights_matched) as total_flights
    from {{ ref('stg_punctuality_data') }}
    group by year
)

select
    year,
    round(safe_divide(total_charter_flights, total_flights), 2) as fraction_charter_flights,
    round(safe_divide(total_scheduled_flights, total_flights), 2) as fraction_scheduled_flights,
    total_flights
from yearly_counts
order by year desc