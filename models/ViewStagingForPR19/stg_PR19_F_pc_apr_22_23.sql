select unique_id
    ,'2022-23' year
    ,'Not sure' submission_status
    ,amp.amp_id
    ,CAST(NULL as varchar(max)) as company_type
    ,company
    ,CAST(NULL as varchar(max)) as element_name
    ,CAST(NULL as varchar(max)) as element_acronym
    ,outcome
    ,pc_ref
    ,CAST(NULL as varchar(max)) as annex
    ,pc_name performance_commitment
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
    ,[isnumeric_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
    ,[onlynumeric_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
    ,[isnumeric_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
    ,[onlynumeric_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
    ,[isnumeric_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
    ,[onlynumeric_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
    ,[isnumeric_underp_payment_incentive_enhanced_underp_payment]
    ,[onlynumeric_underp_payment_incentive_enhanced_underp_payment]
    ,[isnumeric_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
    ,[onlynumeric_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
    ,[isnumeric_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
    ,[onlynumeric_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
    ,[isnumeric_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
    ,[onlynumeric_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
    ,[isnumeric_outp_payment_incentive_enhanced_outp_payment]
    ,[onlynumeric_outp_payment_incentive_enhanced_outp_payment]     from {{ ref('PR19FinalCSVcreatedbyPythonView') }}
         	cross join {{ ref('D_Ofwat_amp') }} amp
	where amp.amp_name='AMP7'
