select unique_id
    ,'2017-18' year
    ,'Actual' submission_status
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
    ,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2017_2017_18] notional_outperformance_payment_or_underperformance_payment_accrued
    ,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2017_gbpm_2017_18] notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
    ,[outp_payment_or_underp_payment_in_period_odis_2017_18] outperformance_payment_or_underperformance_payment_in_period_ODI
    ,[outp_payment_or_underp_payment_in_period_odis_gbpm_2017_18] outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
    ,[pcl_2017_18] pcl
    ,[pcl_met_2017_18] pcl_met
    ,[performance_level_actual_2017_18] performance_level_actual
    ,[total_amp6_outp_payment_or_underp_payment_31_march_2020_forecast_2017_18] Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,[total_amp6_outp_payment_or_underp_payment_31_march_2020_forecast_gbpm_2017_18] Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    ,[financial_odi_2017_18] financial_odi
    ,[underp_payment_collar_2017_18] underp_payment_collar
    ,[underp_payment_deadband_2017_18] underp_payment_deadband
    ,[outp_payment_deadband_2017_18] outp_payment_deadband
    ,[outp_payment_cap_2017_18] outp_payment_cap
    ,'N/A' enhanced_underp_payment_collar
    ,'N/A' standard_underp_payment_collar
    ,'N/A' standard_outp_payment_cap
    ,'N/A' enhanced_outp_payment_cap
    from {{ ref('PR14FinalCSVcreatedbyPythonView') }}
