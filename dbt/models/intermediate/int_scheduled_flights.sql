{{ config(materialized='view') }}

with scheduled as (
	select *
	from {{ ref('stg_punctuality_data') }}
	where scheduled_charter = 'S'
),
ranked as (
	select
		s.*,
		row_number() over (
			partition by unique_row_id
			order by reporting_period_date desc, year desc, month desc
		) as rn
	from scheduled s
)

select
	unique_row_id,
	year,
	month,
	reporting_period_date,
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
from ranked
where rn = 1