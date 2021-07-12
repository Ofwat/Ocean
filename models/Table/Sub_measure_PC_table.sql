with sub_measure_apr_union as (
    select * from {{ ref('stg_Sub_measure_apr_union') }}
),
dim_sub_measure as (
    select * from {{ ref('D_Sub_measure_table') }}
),
pc_company_pr as (
    select * from {{ ref('D_PC_company_pr_table') }}
),
sub_measure_category_table as (
    select * from {{ ref('D_Sub_measure_category_table') }}
),
submission_status as (
    select * from {{ ref('D_Submission_status_table') }}
),
year_table as (
        select * from {{ ref('D_Year_table') }}
),

final as (
select sub_measure_apr_union.unique_id
,dim_sub_measure.D_sub_measure_id
,dim_sub_measure.performance_commitment
,dim_sub_measure.sub_measure_category
,dim_sub_measure.sub_measure
,submission_status.submission_status_id
,submeasure_reference_performance_level
,submeasure_high
,submeasure_low
,performance_level_actual_submeasures
,year_table.year
,year_table.year_id
FROM sub_measure_apr_union
    left join dim_sub_measure
        on isnull(sub_measure_apr_union.unique_id,'unique_id') = isnull(dim_sub_measure.unique_id,'unique_id')
        and isnull(sub_measure_apr_union.sub_measure_id,'sub_measure_id') = isnull(dim_sub_measure.sub_measure_id,'sub_measure_id')
        and isnull(sub_measure_apr_union.sub_measure_category,'sub_measure_category') = isnull(dim_sub_measure.sub_measure_category,'sub_measure_category')
        --and isnull(sub_measure_apr_union.sub_measure,'sub_measure') = isnull(sub_measure.sub_measure,'sub_measure')
    left join sub_measure_category_table
        on isnull(sub_measure_apr_union.sub_measure_category,'sub_measure_category') = isnull(sub_measure_category_table.sub_measure_category,'sub_measure_category')
    left join pc_company_pr
        on isnull(sub_measure_apr_union.unique_id,'unique_id') = isnull(pc_company_pr.unique_id,'unique_id')
    left join submission_status
        on isnull(sub_measure_apr_union.submission_status,'submission_status') = isnull(submission_status.submission_status_name,'submission_status')
    left join year_table
        on isnull(sub_measure_apr_union.year,'year') = isnull(year_table.year,'year')
)

select * from final
