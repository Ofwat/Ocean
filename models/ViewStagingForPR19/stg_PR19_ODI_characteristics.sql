with source_update as (
select * from {{ source('generated_sources', 'PR19FinalCSVcreatedbyPython') }}
)
SELECT distinct(odi_type)
,odi_form
,odi_timing
FROM source_update