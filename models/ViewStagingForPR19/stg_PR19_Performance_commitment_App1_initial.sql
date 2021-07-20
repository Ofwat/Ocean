with pr19 as (
    select * from {{ source('generated_sources', 'PR19App1CSVcreatedbyPython') }}
)

select 'App1' sheet
    ,performance_commitment
    ,primary_category primary_category
    ,pc_unit pc_unit
    ,pc_unit_description pc_unit_description
    ,decimal_places decimal_places
    ,pc_short_description
    from (select performance_commitment
        ,primary_category
        ,pc_unit
        ,pc_unit_description
        ,decimal_places
        ,pc_short_description from pr19
        group by performance_commitment
            ,primary_category
            ,pc_unit
            ,pc_unit_description
            ,decimal_places
            ,pc_short_description
        having performance_commitment is not null
        )p
