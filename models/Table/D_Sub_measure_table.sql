with submeasure as (
    select * from {{ source('nw', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
),
pccompanyamp as (
    select * from {{ ref('D_PC_company_amp') }}
),
company as (
    select * from {{ ref('D_Water_company') }}
),
element as (
    select * from {{ ref('D_Element') }}
),
pc as (
    select * from {{ ref('D_Performance_commitment') }}
),

final as (
    select  
        {{dbt_utils.hash(dbt_utils.concat(['submeasure.unique_id','submeasure.sub_measure_id','submeasure.company']))}} [D_sub_measure_id],
        submeasure.unique_id,
        company_type,
        company,
        submeasure.element_acronym,
        submeasure.pc_ref,
        performance_commitment,
        odi_type,
        primary_category,
        pc_unit_description,
        sub_measure_id,
        sub_measure,
        sub_measure_category,
        sub_measure_weighting,
        decimal_places,
        pc_unit,
        CAST(submeasure_performace_level_reference_regulatory_output_during_2010_15 as float) submeasure_performace_level_reference_regulatory_output_during_2010_15,
        /*ODP-189 This field has text as well
        so converting the column to isnumeric and making one more column to have only text*/
        submeasure_performace_level_reference_expected_performance_by_2014_15,
        isnumeric(submeasure_performace_level_reference_expected_performance_by_2014_15) numeric_submeasure_performace_level_reference_expected_performance_by_2014_15,
        case when isnumeric(submeasure_performace_level_reference_expected_performance_by_2014_15)=0
        then submeasure_performace_level_reference_expected_performance_by_2014_15
        else null end text_present_submeasure_performace_level_reference_expected_performance_by_2014_15,
        /*ODP-191*/
        CAST(submeasure_high_reference_regulatory_output_during_2010_15 as float) submeasure_high_reference_regulatory_output_during_2010_15,
        /*this value is null*/
        CAST(submeasure_high_reference_expected_performance_by_2014_15 as float) submeasure_high_reference_expected_performance_by_2014_15,
        CAST(submeasure_low_reference_regulatory_output_during_2010_15 as float) submeasure_low_reference_regulatory_output_during_2010_15,
        CAST(submeasure_low_reference_expected_performance_by_2014_15 as float) submeasure_low_reference_expected_performance_by_2014_15,
        failure_threshold_for_AMP6,
        submeasure.direction_of_improving_performance
    from submeasure 
    join pccompanyamp on pccompanyamp.unique_id = submeasure.unique_id
    join company on company.water_company_acronym = submeasure.company
    join element on element.element_acronym = submeasure.element_acronym
    where submeasure.sub_measure_id <> '00'
)

select * from final
