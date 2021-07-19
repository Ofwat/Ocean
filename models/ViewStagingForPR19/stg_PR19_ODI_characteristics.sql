with source_update as (
select * from {{ source('generated_sources', 'PR19App1CSVcreatedbyPython') }}
)
SELECT odi_type
    ,{{dbt_utils.hash(dbt_utils.concat(['odi_type','odi_form','odi_timing']))}} ODI_characteristics_id
    ,odi_form
    ,odi_timing
FROM source_update group by  odi_type,odi_form,odi_timing