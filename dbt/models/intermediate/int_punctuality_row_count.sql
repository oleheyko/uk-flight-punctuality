{{ config(materialized='table') }}

select
    count(*) as unioned_row_count
from {{ ref('stg_punctuality_data') }}
