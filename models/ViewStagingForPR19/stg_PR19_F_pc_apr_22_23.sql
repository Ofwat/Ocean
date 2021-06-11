with source_update as (
    select * from {{ source('nw', 'PR19FinalCSVcreatedbyPython') }}
)
select unique_id
,'2022-23' year
,'Past performance' submission_status
,amp.amp_id
,CAST(NULL as varchar(max)) as company_type
,company
,CAST(NULL as varchar(max)) as element_name
,CAST(NULL as varchar(max)) as element_acronym
,outcome
,pc_ref
,CAST(NULL as varchar(max)) as annex
,performance_commitment
,odi_type
,odi_form
,odi_timing
,CAST(NULL as varchar(max)) as in_period_odi
,CAST(NULL as varchar(max)) as vanilla_odi
,primary_category
,pc_unit
,pc_unit_description
,decimal_places
,direction_of_improving_performance
,CAST(NULL as varchar(max)) as notional_outperformance_payment_or_underperformance_payment_accrued
,CAST(NULL as varchar(max)) as notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
,CAST(NULL as varchar(max)) as outperformance_payment_or_underperformance_payment_in_period_ODI
,CAST(NULL as varchar(max)) as outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
,[pcl_2022_23] pcl
,CAST(NULL as varchar(max)) as pcl_met
,CAST(NULL as varchar(max)) as performance_level_actual
,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
,[financial_odi_2022_23] financial_odi
,CAST(NULL as varchar(max)) as underp_payment_collar
,CAST(NULL as varchar(max)) as underp_payment_deadband
,CAST(NULL as varchar(max)) as outp_payment_deadband
,CAST(NULL as varchar(max)) as outp_payment_cap
,[enhanced_underp_payment_collar_2022_23] enhanced_underp_payment_collar
,[standard_underp_payment_collar_2022_23] standard_underp_payment_collar
,[standard_outp_payment_cap_2022_23] standard_outp_payment_cap
,[standard_outp_payment_cap_2022_23] enhanced_outp_payment_cap
from source_update
cross join {{ ref('D_Ofwat_amp') }} amp
where amp.amp_name='AMP7'
