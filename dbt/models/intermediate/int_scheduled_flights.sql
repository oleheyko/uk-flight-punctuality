{{ config(materialized='view') }}

select *
from {{ ref('stg_punctuality_data') }}
where scheduled_charter = 'S'