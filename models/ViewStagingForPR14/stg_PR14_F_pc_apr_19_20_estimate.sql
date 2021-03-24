select unique_id
    ,'2019-20' year
    ,'Estimate' submission_status
	,company_type
	,company
	,element_name
	,element_acronym
	,outcome
	,pc_ref
	,annex
	,performance_commitment
	,odi_type
	,odi_form
	,'N/A' odi_timing
	,in_period_odi
	,vanilla_odi
	,primary_category
	,pc_unit
	,pc_unit_description
	,decimal_places
	,direction_of_improving_performance
	,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2020_estimates_2019_20] notional_outperformance_payment_or_underperformance_payment_accrued
    ,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2020_gbpm_estimates_2019_20] notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
    ,[outp_payment_or_underp_payment_in_period_odis_estimates_2019_20] outperformance_payment_or_underperformance_payment_in_period_ODI
    ,[outp_payment_or_underp_payment_in_period_odis_gbpm_estimates_2019_20] outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
	,'N/A' pcl
	,[pcl_met_estimates_2019_20] pcl_met
    ,[performance_level_actual_estimates_2019_20] performance_level_actual
	,'N/A' Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,'N/A' Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    ,'N/A' financial_odi
    ,'N/A' underp_payment_collar
    ,'N/A' underp_payment_deadband
    ,'N/A' outp_payment_deadband
    ,'N/A' outp_payment_cap
    ,'N/A' enhanced_underp_payment_collar
    ,'N/A' standard_underp_payment_collar
    ,'N/A' standard_outp_payment_cap
    ,'N/A' enhanced_outp_payment_cap
    from {{ ref('PR14FinalCSVcreatedbyPythonView') }}
