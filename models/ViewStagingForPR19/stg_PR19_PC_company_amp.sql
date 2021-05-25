with pr19 as (
    select * from {{ ref('PR19FinalCSVcreatedbyPythonView') }}
),
company as (
    select * from {{ ref('D_Water_company') }}
),
pc as (
    select * from {{ ref('stg_PR19_Performance_commitment') }}
),
dateofwat as (
    select * from {{ ref('D_Date_OFWAT') }}
),
amp as (
    select * from {{ ref('D_Ofwat_amp') }}
),

final as (
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.pc_name','pc.primary_category']))}} pc_company_amp_id
    ,pc.performance_commitment_id
    ,pc.pc_name
    ,company.water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    ,common_and_comparable_bespoke_performance_commitment
    ,CAST(NULL as varchar(max)) as annex
    ,direction_of_improving_performance
    ,CAST(NULL as varchar(max)) as drinking_water_quality_compliance
    ,CAST(NULL as varchar(max)) as water_quality_contacts
    , CAST(NULL as varchar(max)) assupply_interruptions_3_hours
    , CAST(NULL as varchar(max)) aspollution_incidents_cat_3
    , CAST(NULL as varchar(max)) asinternal_sewer_flooding
    ,special_cost_factor
    ,scheme_specific_factor
    ,asset_health
    ,nep
    ,AIM
    ,customers_relative_priority
    ,CAST(NULL as varchar(max)) as no_of_sub_measures
    ,standard_odi_cal
    ,standard_odi_operand
    ,standard_odi_operand_note
    ,CAST(NULL as varchar(max)) as [isnumeric_underp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[isnumeric_underp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[isnumeric_underp_payment3_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[isnumeric_underp_payment4_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[isnumeric_outp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[isnumeric_outp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_underp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_underp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_underp_payment3_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_underp_payment4_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_outp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as[onlynumeric_outp_payment2_incentive_rate_gbpm]
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
      ,[onlynumeric_outp_payment_incentive_enhanced_outp_payment]
    ,[price_control_allocation_water_resources]
    ,[price_control_allocation_water_network_plus]
    ,[price_control_allocation_wastewater_network_plus]
    ,[price_control_allocation_bioresources_sludge]
    ,[price_control_allocation_residential_retail]
    ,[price_control_allocation_business_retail]
    ,[price_control_allocation_direct_procurement_for_customers]
    ,[price_control_allocation_dummy_control]
    from pr19
        left join pc on pr19.pc_name=pc.pc_name
        and pr19.pc_unit=pc.pc_unit
        and pr19.pc_unit_description=pc.pc_unit_description
        and pr19.decimal_places=pc.decimal_places
        and pr19.primary_category=pc.primary_category
        left join company on pr19.company=company.water_company_acronym
        cross join amp
        where amp.amp_name = 'AMP7'
)

select * from final