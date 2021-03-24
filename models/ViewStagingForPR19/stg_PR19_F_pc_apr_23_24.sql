select unique_id
    ,'2023-24' year
    ,'Not sure' submission_status
    ,'' company_type
    ,company
    ,'' element_name
    ,'' element_acronym
    ,outcome
    ,pc_ref
    ,'' annex
    ,pc_name performance_commitment
    ,odi_type
    ,odi_form
    ,'' odi_timing
    ,'' in_period_odi
    ,'' vanilla_odi
    ,primary_category
    ,pc_unit
    ,pc_unit_description
    ,decimal_places
    ,direction_of_improving_performance
    ,'' notional_outperformance_payment_or_underperformance_payment_accrued
    ,'' notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
    ,'' outperformance_payment_or_underperformance_payment_in_period_ODI
    ,'' outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
    ,[pcl_2023_24] pcl
    ,'' pcl_met
    ,'' performance_level_actual
    ,'' Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,'' Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    ,[financial_odi_2023_24] financial_odi
    ,'' underp_payment_collar
    ,'' underp_payment_deadband
    ,'' outp_payment_deadband
    ,'' outp_payment_cap
    ,[enhanced_underp_payment_collar_2023_24] enhanced_underp_payment_collar
    ,[standard_underp_payment_collar_2023_24] standard_underp_payment_collar
    ,[standard_outp_payment_cap_2023_24] standard_outp_payment_cap
    ,[standard_outp_payment_cap_2023_24] enhanced_outp_payment_cap
     from {{ ref('PR19FinalCSVcreatedbyPythonView') }}
