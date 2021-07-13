with pr19 as (
    select * from {{ source('generated_sources', 'PR19FinalCSVcreatedbyPython') }}
)

select  {{dbt_utils.hash(dbt_utils.concat(['performance_commitment','primary_category','pc_unit','pc_unit_description','decimal_places', 'pc_short_description']))}} performance_commitment_id
    ,performance_commitment
    ,primary_category primary_category
    ,pc_unit pc_unit
    ,pc_unit_description pc_unit_description
    ,decimal_places decimal_places
    ,pc_short_description
    ,'App1b' sheet
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
        )p
