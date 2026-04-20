-- Fails if key percent fields fall outside the 0-100 range
select *
from {{ ref('stg_punctuality_data') }}
where
    flights_cancelled_percent < 0
    or flights_cancelled_percent > 100
    or flights_unmatched_percent < 0
    or flights_unmatched_percent > 100
    or more_than_360_mins_late_percent < 0
    or more_than_360_mins_late_percent > 100
