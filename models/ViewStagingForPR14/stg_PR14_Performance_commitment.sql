with pr14 as (
    select * from {{ source('generated_sources', 'PR14FinalCSVcreatedbyPython') }}
)

select  {{dbt_utils.hash(dbt_utils.concat(['pc_name','primary_category','pc_unit','pc_unit_description','decimal_places']))}} performance_commitment_id
    ,pc_name performance_commitment
    ,primary_category primary_category
    ,pc_unit pc_unit
    ,pc_unit_description pc_unit_description
    ,decimal_places decimal_places
    ,CAST(NULL as varchar(max)) as pc_short_description
    from (select ltrim(right(performance_commitment, len(performance_commitment) - charindex(':',performance_commitment))) pc_name
        ,primary_category
        ,pc_unit
        ,pc_unit_description
        ,decimal_places from pr14
        group by ltrim(right(performance_commitment, len(performance_commitment) - charindex(':',performance_commitment)))
            ,primary_category
            ,pc_unit
            ,pc_unit_description
            ,decimal_places
        )p
