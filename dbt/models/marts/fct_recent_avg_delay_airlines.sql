-- Calculate average delay for the busiest 20 airlines in the most recent year
{{ config(materialized='view') }}

select
    airline_name,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
from {{ ref('stg_punctuality_data') }}
where year = (select max(year) from {{ ref('int_scheduled_flights') }})
and airline_name in (select airline_name from {{ ref('int_recent_busiest_airlines') }})
group by airline_name
order by avg_delay_mins desc
