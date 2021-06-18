with sub_measure_apr_union as (
    select * from {{ ref('stg_Sub_measure_apr_union') }}
),
sub_measure as (
    select * from {{ ref('D_Sub_measure_table') }}
),
pc_company_pr as (
    select * from {{ ref('D_PC_company_pr_table') }}
),
sub_measure_category as (
    select * from {{ ref('D_Sub_measure_category_table') }}
),
submission as (
    select * from {{ ref('D_Submission_status_table') }}
),

final as (
select sub_measure_apr_union.unique_id
,sub_measure.performance_commitment
,sub_measure.sub_measure_category
,sub_measure.sub_measure
,submission_status
,submeasure_reference_performance_level
,submeasure_high
,submeasure_low
,performance_level_actual_submeasures
,year
FROM sub_measure_apr_union
    left join sub_measure
    on isnull(sub_measure_apr_union.unique_id,'D_sub_measure_id') = isnull(sub_measure.D_sub_measure_id,'D_sub_measure_id')
    and isnull(sub_measure_apr_union.sub_measure_category,'sub_measure_category') = isnull(sub_measure.sub_measure_category,'sub_measure_category')
    and isnull(sub_measure_apr_union.sub_measure,'sub_measure') = isnull(sub_measure.sub_measure,'sub_measure')
    left join pc_company_pr on isnull(sub_measure_apr_union.unique_id,'unique_id') = isnull(pc_company_pr.unique_id,'unique_id')
    left join  submission on isnull(sub_measure_apr_union.submission_status,'submission_status') = isnull(replace(submission.submission_status_description,'APR',''),'submission_status')
)

select * from final
