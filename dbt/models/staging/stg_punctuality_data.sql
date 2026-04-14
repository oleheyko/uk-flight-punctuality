{{ config(
    materialized='table',
    partition_by={
        'field': 'reporting_period_date',
        'data_type': 'date'
    },
    cluster_by=['origin_destination', 'airline_name']
) }}

select
    year,
    month,
    date(year, month, 1) as reporting_period_date,
    reporting_airport,
    origin_destination_country,
    origin_destination,
    airline_name,
    scheduled_charter,
    number_flights_matched,
    actual_flights_unmatched,
    early_to_15_mins_late_percent,
    `16_to_30_mins_late_percent`,
    `31_to_60_mins_late_percent`,
    `61_to_180_mins_late_percent`,
    `181_to_360_mins_late_percent`,
    more_than_360_mins_late_percent,
    average_delay_mins,
    planned_flights_unmatched,
    more_than_15_mins_early_percent,
    `15_mins_early_to_1_minute_early_percent`,
    `0_to_15_mins_late_percent`,
    flights_unmatched_percent,
    flights_cancelled_percent,
    number_flights_cancelled
from {{ source('source_data', 'punctuality_data_all_years') }}
