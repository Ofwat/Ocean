with submeasure as (
    select * from {{ source('nw', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
),

final as (
    select {{dbt_utils.hash(dbt_utils.concat(['submeasure.unique_id']))}} [D_sub_measure_id]
    ,unique_id
    ,company_type
    ,company
    ,submeasure.element_acronym
    ,pc_ref
    ,performance_commitment
    ,odi_type
    ,primary_category
    ,pc_unit_description
    ,sub_measure_id
    ,sub_measure
    ,sub_measure_category
    ,sub_measure_weighting
    ,pc_unit
    ,decimal_places
    ,actual_performance_compared_with_previous_actual_performance_2014_15_to_2015_16
    ,actual_performance_compared_with_previous_actual_performance_2015_16_to_2016_17
    ,actual_performance_compared_with_previous_actual_performance_2016_17_to_2017_18
    ,actual_performance_compared_with_previous_actual_performance_2017_18_to_2018_19
    ,actual_performance_compared_with_previous_actual_performance_2018_19_to_2019_20
    ,actual_performance_compared_with_previous_actual_performance_2014_15_to_2016_17_amp_so_far
    from submeasure 
    where submeasure.sub_measure_id <> '00'
)

select * from final
