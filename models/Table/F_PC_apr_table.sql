with Fpcaprunion as (
    select * from {{ ref('stg_F_pc_apr_union') }}
),
company as (
    select * from {{ ref('D_Water_company_table') }}
),
pccompamp as (
    select * from {{ ref('stg_PC_company_amp_union') }}
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
odi_characteristics as (
    select * from {{ ref('D_ODI_characteristics_table') }}
),
renamed as (
    select year OFWAT_Year
    ,Fpcaprunion.amp_id
    ,Fpcaprunion.submission_status
    ,pccompamp.unique_id
    ,company.water_company_id
    ,pccompamp.pc_company_amp_id
    ,Submission_status_id
    ,element.element_id
    ,odi_characteristics.ODI_characteristics_id
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
        left join pccompamp on isnull(Fpcaprunion.unique_id,'unique_id') = isnull(pccompamp.unique_id,'unique_id')
        left join element on isnull(Fpcaprunion.element_acronym,'element_acronym') = isnull(element.element_acronym,'element_acronym')
        left join submission on isnull(Fpcaprunion.submission_status,'submission_status') = isnull(submission.submission_status_name,'submission_status')
        left join financialincentive on coalesce([notional_outperformance_payment_or_underperformance_payment_accrued],
                [outperformance_payment_or_underperformance_payment_in_period_ODI]) = financialincentive.financial_incentive_type
                and 
                (case when [notional_outperformance_payment_or_underperformance_payment_accrued] is not null then 'Notional Period'
                when [outperformance_payment_or_underperformance_payment_in_period_ODI] is not null then 'IN Period ODI'
                else null end) = financialincentive.Incentive_Period
        left join odi_characteristics on isnull(Fpcaprunion.odi_form,'odi_characteristics') = isnull(odi_characteristics.odi_form,'odi_characteristics')
        and isnull(Fpcaprunion.odi_type,'odi_type') = isnull(odi_characteristics.odi_type,'odi_type')
        and isnull(Fpcaprunion.odi_timing,'odi_timing') = isnull(odi_characteristics.odi_timing,'odi_timing')
        where Fpcaprunion.company is not null
)

select * from renamed

