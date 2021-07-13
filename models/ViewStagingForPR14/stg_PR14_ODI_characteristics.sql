with source_update as (
    select * from {{ source('generated_sources', 'PR14FinalCSVcreatedbyPython') }}
)
SELECT odi_type
    ,{{dbt_utils.hash(dbt_utils.concat(['odi_type','odi_form']))}} ODI_characteristics_id
    ,odi_form
    ,CAST(NULL as varchar(max)) as odi_timing
    ,'PC list' sheet
FROM source_update group by  odi_type,odi_form