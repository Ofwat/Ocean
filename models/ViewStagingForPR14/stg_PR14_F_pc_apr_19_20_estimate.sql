select unique_id
    ,'2019-20' year
    ,'Estimate' submission_status
	,amp.amp_id
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
	,CAST(NULL as varchar(max)) as odi_timing
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
	,CAST(NULL as varchar(max)) as pcl
	,[pcl_met_estimates_2019_20] pcl_met
    ,[performance_level_actual_estimates_2019_20] performance_level_actual
	,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    ,CAST(NULL as varchar(max)) as financial_odi
    ,CAST(NULL as varchar(max)) as underp_payment_collar
    ,CAST(NULL as varchar(max)) as underp_payment_deadband
    ,CAST(NULL as varchar(max)) as outp_payment_deadband
    ,CAST(NULL as varchar(max)) as outp_payment_cap
    ,CAST(NULL as varchar(max)) as enhanced_underp_payment_collar
    ,CAST(NULL as varchar(max)) as standard_underp_payment_collar
    ,CAST(NULL as varchar(max)) as standard_outp_payment_cap
    ,CAST(NULL as varchar(max)) as enhanced_outp_payment_cap
    from {{ ref('PR14FinalCSVcreatedbyPythonView') }}
    	cross join {{ ref('D_Ofwat_amp') }} amp
	where amp.amp_name='AMP6'