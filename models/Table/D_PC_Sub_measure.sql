with sub_measure_apr_union as (
    select * from {{ ref('stg_Sub_measure_apr_union') }}
),
sub_measure as (
    select * from {{ ref('D_Sub_measure') }}
),
company as (
    select * from {{ ref('D_Water_company') }}
),
pc_company_amp as (
    select * from {{ ref('D_PC_company_amp') }}
),
sub_measure_category as (
    select * from {{ ref('D_Sub_measure_category') }}
),
submission as (
    select * from {{ ref('D_Submission_status') }}
),

final as (
select 
    sub_measure.D_sub_measure_id
    ,year 
    ,submission_status
    ,pc_company_amp.unique_id
    ,company.water_company_id
    ,pc_company_amp.pc_company_amp_id
    ,sub_measure_category.sub_measure_category_id
    ,submeasure_performace_level
    ,CAST(submeasure_high as float) submeasure_high
    ,CAST(submeasure_low as float) submeasure_low
    ,CAST(submeasure_high as float) performance_level_actual
    ,performance_level_met
FROM sub_measure_apr_union
    inner join sub_measure on 
        {{dbt_utils.hash(dbt_utils.concat(['sub_measure_apr_union.unique_id','sub_measure_apr_union.sub_measure_id','sub_measure_apr_union.company']))}} 
        = sub_measure.D_sub_measure_id
    left join company on sub_measure_apr_union.company = company.water_company_acronym
    left join pc_company_amp on sub_measure_apr_union.[unique_id] = pc_company_amp.unique_id
    left join sub_measure_category on sub_measure_apr_union.[sub_measure_category] = sub_measure_category.sub_measure_category
    left join  submission on sub_measure_apr_union.[submission_status]=replace(submission.submission_status_description,'APR','')
)

select * from final
