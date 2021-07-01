with Fpcaprunion as (
    select * from {{ ref('stg_F_pc_apr_union') }}
),
company as (
    select * from {{ ref('D_Water_company_table') }}
),
pccomppr as (
    select * from {{ ref('D_PC_company_pr') }}
),
financialincentive as (
    select * from {{ ref('D_Financial_incentive_type_table') }}
),
element as (
    select * from {{ ref('D_Element_table') }}
),
submission as (
    select * from {{ ref('D_Submission_status_table') }}
),
pr as (
    select * from {{ ref('D_Price_review_table') }}
),
Performance_commitment as (
    select * from {{ ref('D_Performance_commitment_table') }}
),
yeartable as (
    select * from {{ ref('D_Year_table') }}
),
renamed as (
    select yeartable.year
    ,yeartable.year_id
    ,Fpcaprunion.price_review
    ,Fpcaprunion.submission_status
    ,pccomppr.unique_id
    ,company.water_company_id
    ,pccomppr.pc_company_pr_id
    ,Submission_status_id
    ,element.element_id
    ,underp_payment_collar
    ,underp_payment_deadband
    ,outp_payment_deadband
    ,outp_payment_cap
    ,pcl 
    ,pcl_met
    ,enhanced_underp_payment_collar
    ,performance_level_actual pcl_Actual
    ,isnumeric(performance_level_actual) numeric_pcl_actual
    ,case when isnumeric(performance_level_actual) =1 then performance_level_actual 
    else null end numeric_derived_pcl_actual
    ,financialincentive.financial_incentive_id
    ,coalesce([notional_outperformance_payment_or_underperformance_payment_accrued_GBPm],
    [outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm]) financial_incentive_payment_GBPm
    ,Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    from Fpcaprunion 
        left join company  on isnull(Fpcaprunion.company,'company') = isnull(company.water_company_acronym,'company')
        left join pccomppr on isnull(Fpcaprunion.unique_id,'unique_id') = isnull(pccomppr.unique_id,'unique_id')
        left join element on isnull(Fpcaprunion.element_acronym,'element_acronym') = isnull(element.element_acronym,'element_acronym')
        left join submission on isnull(Fpcaprunion.submission_status,'submission_status') = isnull(submission.submission_status_name,'submission_status')
        left join financialincentive on coalesce([notional_outperformance_payment_or_underperformance_payment_accrued],
                [outperformance_payment_or_underperformance_payment_in_period_ODI]) = financialincentive.financial_incentive_type
                and 
                (case when [notional_outperformance_payment_or_underperformance_payment_accrued] is not null then 'Notional Period'
                when [outperformance_payment_or_underperformance_payment_in_period_ODI] is not null then 'IN Period ODI'
                else null end) = financialincentive.Incentive_Period
        left join yeartable on yeartable.year = Fpcaprunion.year
        where Fpcaprunion.company is not null
)

select * from renamed

