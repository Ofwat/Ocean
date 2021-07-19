with source_update as (
    select * from {{ source('generated_sources', 'PR14BaseCSVcreatedbyPython') }}
)
select unique_id
    ,'2019-20' year
    ,'Actual' submission_status
    ,'PR14' price_review
    ,'PC list' sheet
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
    ,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2020_2019_20] notional_outperformance_payment_or_underperformance_payment_accrued
    ,[notional_outp_payment_or_underp_payment_accrued_at_31_march_2020_gbpm_2019_20] notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
    ,[outp_payment_or_underp_payment_in_period_odis_2019_20] outperformance_payment_or_underperformance_payment_in_period_ODI
    ,[outp_payment_or_underp_payment_in_period_odis_gbpm_2019_20] outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
	  ,[pcl_2019_20] pcl
    ,[pcl_met_2019_20] pcl_met
    ,[performance_level_actual_2019_20] performance_level_actual
    ,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
    ,CAST(NULL as varchar(max)) as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
    ,[financial_odi_2019_20] financial_odi
    ,[underp_payment_collar_2019_20] underp_payment_collar
    ,[underp_payment_deadband_2019_20] underp_payment_deadband
    ,[outp_payment_deadband_2019_20] outp_payment_deadband
    ,[outp_payment_cap_2019_20] outp_payment_cap
    ,CAST(NULL as varchar(max)) as enhanced_underp_payment_collar
    ,CAST(NULL as varchar(max)) as standard_underp_payment_collar
    ,CAST(NULL as varchar(max)) as standard_outp_payment_cap
    ,CAST(NULL as varchar(max)) as enhanced_outp_payment_cap
    from source_update
	where unique_id is not null
