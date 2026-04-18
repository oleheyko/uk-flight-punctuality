-- Generate a table of distinct reporting airports from the staging punctuality data, filtered to the most recent year.

{{ config(materialized='view') }}

select distinct reporting_airport
from {{ ref('stg_punctuality_data') }}
where year = (select max(year) from {{ ref('stg_punctuality_data') }})
order by reporting_airport