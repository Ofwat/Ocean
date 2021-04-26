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
      ,isnumeric(underp_payment1_incentive_rate_gbpm) numeric_underp_payment1_incentive_rate_gbpm
      ,case when isnumeric(underp_payment1_incentive_rate_gbpm) =1 then underp_payment1_incentive_rate_gbpm 
      else null end underp_payment1_incentive_rate_gbpm_comments
      ,[underp_payment2_incentive_rate_gbpm]
      ,isnumeric(underp_payment2_incentive_rate_gbpm) numeric_underp_payment2_incentive_rate_gbpm
      ,case when isnumeric(underp_payment2_incentive_rate_gbpm) =1 then underp_payment2_incentive_rate_gbpm 
      else null end underp_payment2_incentive_rate_gbpm_comments
      ,[underp_payment3_incentive_rate_gbpm] numeric_underp_payment3_incentive_rate_gbpm
      ,case when isnumeric(underp_payment3_incentive_rate_gbpm) =1 then underp_payment3_incentive_rate_gbpm 
      else null end underp_payment3_incentive_rate_gbpm_comments
      ,[underp_payment4_incentive_rate_gbpm]
            ,[underp_payment4_incentive_rate_gbpm] numeric_underp_payment4_incentive_rate_gbpm
      ,case when isnumeric(underp_payment4_incentive_rate_gbpm) =1 then underp_payment4_incentive_rate_gbpm 
      else null end underp_payment4_incentive_rate_gbpm_comments
      ,[outp_payment1_incentive_rate_gbpm]
            ,[outp_payment1_incentive_rate_gbpm] numeric_outp_payment1_incentive_rate_gbpm
      ,case when isnumeric(outp_payment1_incentive_rate_gbpm) =1 then outp_payment1_incentive_rate_gbpm 
      else null end outp_payment1_incentive_rate_gbpm_comments
      ,[outp_payment2_incentive_rate_gbpm]
            ,[outp_payment2_incentive_rate_gbpm] numeric_outp_payment2_incentive_rate_gbpm
      ,case when isnumeric(outp_payment2_incentive_rate_gbpm) =1 then outp_payment2_incentive_rate_gbpm 
      else null end outp_payment2_incentive_rate_gbpm_comments
      ,[underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
            ,[underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply] numeric_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply
      ,case when isnumeric(underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply) =1 then underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply 
      else null end underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply_comments
      ,[underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
                  ,[underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply] numeric_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply
      ,case when isnumeric(underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply) =1 then underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply 
      else null end underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply_comments
      ,[underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
                  ,[underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply] numeric_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply
      ,case when isnumeric(underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply) =1 then underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply 
      else null end underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply_comments
      ,[underp_payment_incentive_enhanced_underp_payment]
                  ,[underp_payment_incentive_enhanced_underp_payment] numeric_underp_payment_incentive_enhanced_underp_payment
      ,case when isnumeric(underp_payment_incentive_enhanced_underp_payment) =1 then underp_payment_incentive_enhanced_underp_payment 
      else null end underp_payment_incentive_enhanced_underp_payment_comments
      ,[outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
                  ,[outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply] numeric_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply
      ,case when isnumeric(outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply) =1 then outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply 
      else null end outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply_comments
      ,[outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
                  ,[outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply] numeric_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply
      ,case when isnumeric(outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply) =1 then outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply 
      else null end outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply_comments
      ,[outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
                  ,[outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply] numeric_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply
      ,case when isnumeric(outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply) =1 then outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply 
      else null end outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply_comments
      ,[outp_payment_incentive_enhanced_outp_payment]
                  ,[outp_payment_incentive_enhanced_outp_payment] numeric_outp_payment_incentive_enhanced_outp_payment
      ,case when isnumeric(outp_payment_incentive_enhanced_outp_payment) =1 then outp_payment_incentive_enhanced_outp_payment 
      else null end outp_payment_incentive_enhanced_outp_payment_comments
      ,CAST(price_control_allocation_water_resources as float)  [price_control_allocation_water_resources]
      ,CAST(price_control_allocation_water_network_plus as float) [price_control_allocation_water_network_plus]
      ,CAST(price_control_allocation_wastewater_network_plus as float) [price_control_allocation_wastewater_network_plus]
      ,CAST(price_control_allocation_bioresources_sludge as float) [price_control_allocation_bioresources_sludge]
      ,CAST(price_control_allocation_residential_retail as float) [price_control_allocation_residential_retail]
      ,CAST(price_control_allocation_business_retail as float) [price_control_allocation_business_retail]
      ,CAST(price_control_allocation_direct_procurement_for_customers as float) [price_control_allocation_direct_procurement_for_customers]
      ,CAST(price_control_allocation_dummy_control as float) [price_control_allocation_dummy_control]
  FROM {{ ref('stg_PC_company_amp_union')}}