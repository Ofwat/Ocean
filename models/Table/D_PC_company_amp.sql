SELECT [pc_company_amp_id]
      ,[performance_commitment_id]
      ,[pc_name]
      ,[water_company_id]
      ,[unique_id]
      ,[outcome]
      ,[PC_ref]
      ,[common_comparable_bespoke_performance_commitment]
      ,[annex]
      ,[direction_of_improving_performance]
      ,[drinking_water_quality_compliance]
      ,[water_quality_contacts]
      ,[supply_interruptions_3_hours]
      ,[pollution_incidents_cat_3]
      ,[internal_sewer_flooding]
      ,[special_cost_factor]
      ,[scheme_specific_factor]
      ,[asset_health]
      ,[nep]
      ,[AIM]
      ,[customers_relative_priority]
      ,[no_of_sub_measures]
      ,[standard_odi_cal]
      ,[standard_odi_operand]
      ,[standard_odi_operand_note]
      ,[underp_payment1_incentive_rate_gbpm]
      ,[underp_payment2_incentive_rate_gbpm]
      ,[underp_payment3_incentive_rate_gbpm]
      ,[underp_payment4_incentive_rate_gbpm]
      ,[outp_payment1_incentive_rate_gbpm]
      ,[outp_payment2_incentive_rate_gbpm]
      ,[underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
      ,[underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
      ,[underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
      ,[underp_payment_incentive_enhanced_underp_payment]
      ,[outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
      ,[outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
      ,[outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
      ,[outp_payment_incentive_enhanced_outp_payment]
      ,[water_resources]
      ,[water_network_plus]
      ,[wastewater_network_plus]
      ,[bioresources_sludge]
      ,[residential_retail]
      ,[business_retail]
      ,[direct_procurement_for_customers]
      ,[dummy_control]
  FROM {{ ref('stg_PC_company_amp_union')}}