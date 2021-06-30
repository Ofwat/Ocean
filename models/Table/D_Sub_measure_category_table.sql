with source_update as (
    select * from {{ source('generated_sources', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
)

select DISTINCT {{dbt_utils.hash('sub_measure_category')}} [sub_measure_category_id], sub_measure_category
from source_update
where sub_measure_category IS NOT NULL
