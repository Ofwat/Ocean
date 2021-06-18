with sub_measure_apr_union as (
    select * from {{ ref('stg_Sub_measure_apr_union') }}
),
sub_measure as (
    select * from {{ ref('D_Sub_measure_table') }}
),
company as (
    select * from {{ ref('D_Water_company_table') }}
),
pc_company_amp as (
    select * from {{ ref('D_PC_company_amp_table') }}
),
sub_measure_category as (
    select * from {{ ref('D_Sub_measure_category_table') }}
),
submission as (
    select * from {{ ref('D_Submission_status_table') }}
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
    ,performance_level_actual
    ,performance_level_met
FROM sub_measure_apr_union
    left join sub_measure on isnull({{dbt_utils.hash(dbt_utils.concat(['sub_measure_apr_union.unique_id','sub_measure_apr_union.sub_measure_id','sub_measure_apr_union.company']))}},'D_sub_measure_id' ) = isnull(sub_measure.D_sub_measure_id,'D_sub_measure_id')
    left join company on isnull(sub_measure_apr_union.company,'water_company_acronym') = isnull(company.water_company_acronym,'water_company_acronym')
    left join pc_company_amp on isnull(sub_measure_apr_union.unique_id,'unique_id') = isnull(pc_company_amp.unique_id,'unique_id')
    left join sub_measure_category on isnull(sub_measure_apr_union.sub_measure_category,'sub_measure_category') = isnull(sub_measure_category.sub_measure_category,'sub_measure_category')
    left join  submission on isnull(sub_measure_apr_union.submission_status,'submission_status') = isnull(replace(submission.submission_status_description,'APR',''),'submission_status')
)

select * from final
