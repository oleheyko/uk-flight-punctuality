-- Top 20 busiest airlines by number of flights in the most recent year
{{ config(materialized='view') }}

select
    airline_name,
    sum(number_flights_matched) as total_flights
from {{ ref('int_scheduled_flights') }}
where year = (select max(year) from {{ ref('int_scheduled_flights') }})
group by airline_name
order by total_flights desc
limit 20