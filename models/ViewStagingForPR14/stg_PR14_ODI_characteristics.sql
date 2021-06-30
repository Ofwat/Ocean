with source_update as (
    select * from {{ source('generated_sources', 'PR14FinalCSVcreatedbyPython') }}
)
SELECT distinct(odi_type)
      ,odi_form
      ,CAST(NULL as varchar(max)) as odi_timing
  FROM source_update