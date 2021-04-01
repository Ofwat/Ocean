select unique_id
    ,'2014-15' year
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
    ,CAST(NULL as varchar(max)) as odi_timing
    ,in_period_odi
    ,vanilla_odi
    ,primary_category
    ,pc_unit
    ,pc_unit_description
    ,decimal_places
    ,direction_of_improving_performance
    ,CAST(NULL as varchar(max)) as notional_outperformance_payment_or_underperformance_payment_accrued
    ,CAST(NULL as varchar(max)) as notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
    ,CAST(NULL as varchar(max)) as outperformance_payment_or_underperformance_payment_in_period_ODI
    ,CAST(NULL as varchar(max)) as outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
    ,CAST(NULL as varchar(max)) as pcl
    ,CAST(NULL as varchar(max)) as pcl_met
    ,[starting_level_pr14_fd_2014_15] performance_level_actual
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
