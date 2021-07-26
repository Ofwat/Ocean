with pr19 as (
    select * from {{ ref('stg_PR19_PC_company_pr_App1b_initial') }}
)


    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','performance_commitment','primary_category','sheet']))}} pc_company_pr_id
    ,sheet
    ,price_review
    ,performance_commitment_id
    ,performance_commitment
    ,primary_category
    ,pc_short_description
    ,water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    ,ODI_characteristics_id
    ,common_and_comparable_bespoke_performance_commitment as common_comparable_bespoke_performance_commitment
    ,annex
    ,direction_of_improving_performance
    ,PR14_comparative_drinking_water_compliance
    ,PR14_comparative_water_quality_contacts
    ,PR14_comparative_supply_interruptions_3_hours
    ,PR14_comparative_pollution_incidents_cat_3
    ,PR14_comparative_internal_sewer_flooding
    ,special_cost_factor
    ,scheme_specific_factor
    ,asset_health
    ,nep
    ,AIM
    ,customers_relative_priority
    ,no_of_sub_measures
    ,standard_odi_cal
    ,standard_odi_operand
    ,standard_odi_operand_note
    ,[notes_underp_payment1_incentive_rate_gbpm]
    ,[underp_payment1_incentive_rate_gbpm]
    ,[notes_underp_payment2_incentive_rate_gbpm]
    ,[underp_payment2_incentive_rate_gbpm]
    ,[notes_underp_payment3_incentive_rate_gbpm]
    ,[underp_payment3_incentive_rate_gbpm]
    ,[notes_underp_payment4_incentive_rate_gbpm]
    ,[underp_payment4_incentive_rate_gbpm]
    ,[notes_outp_payment1_incentive_rate_gbpm]
    ,[outp_payment1_incentive_rate_gbpm]
    ,[notes_outp_payment2_incentive_rate_gbpm]
    ,[outp_payment2_incentive_rate_gbpm]
    ,notes_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply
    ,underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply
    ,notes_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply
    ,underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply
    ,notes_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply
    ,underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply
    ,notes_underp_payment_incentive_enhanced_underp_payment
    ,underp_payment_incentive_enhanced_underp_payment
    ,notes_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply
    ,outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply
    ,notes_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply
    ,outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply
    ,notes_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply
    ,outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply
    ,notes_outp_payment_incentive_enhanced_outp_payment
    ,outp_payment_incentive_enhanced_outp_payment
    ,price_control_allocation_water_resources
    ,price_control_allocation_water_network_plus
    ,price_control_allocation_wastewater_network_plus
    ,price_control_allocation_bioresources_sludge
    ,price_control_allocation_residential_retail
    ,price_control_allocation_business_retail
    ,price_control_allocation_direct_procurement_for_customers
    ,price_control_allocation_dummy_control
    from pr19
