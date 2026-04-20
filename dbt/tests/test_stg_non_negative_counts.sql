-- Fails if any of the count fields are negative
select *
from {{ ref('stg_punctuality_data') }}
where
    number_flights_matched < 0
    or actual_flights_unmatched < 0
    or planned_flights_unmatched < 0
    or number_flights_cancelled < 0
