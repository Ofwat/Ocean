with pr14 as (
    select * from {{ ref('PR14FinalCSVcreatedbyPythonView') }}
),
company as (
    select * from {{ ref('D_Water_company') }}
),
pc as (
    select * from {{ ref('stg_PR14_Performance_commitment') }}
),
dateofwat as (
    select * from {{ ref('D_Date_OFWAT') }}
),
amp as (
    select * from {{ ref('D_Ofwat_amp') }}
),
element as (
    select * from {{ ref('D_Element') }}
),

final as (
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.pc_name','pc.primary_category']))}} pc_company_amp_id
    ,pc.performance_commitment_id
    ,pc.pc_name
    ,company.water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    ,CAST(NULL as varchar(max)) as common_comparable_bespoke_performance_commitment
    ,annex
    ,direction_of_improving_performance
    ,drinking_water_quality_compliance
    ,water_quality_contacts
    ,supply_interruptions_3_hours
    ,pollution_incidents_cat_3
    ,internal_sewer_flooding
    ,CAST(NULL as varchar(max)) as special_cost_factor
    ,scheme_specific_factor
    ,asset_health
    ,nep
    ,AIM
    ,CAST(NULL as varchar(max)) as customers_relative_priority
    ,no_of_sub_measures
    ,CAST(NULL as varchar(max)) as standard_odi_cal
    ,standard_odi_operand
    ,standard_odi_operand_note
    ,[isnumeric_underp_payment1_incentive_rate_gbpm]
    ,[isnumeric_underp_payment2_incentive_rate_gbpm]
    ,[isnumeric_underp_payment3_incentive_rate_gbpm]
    ,[isnumeric_underp_payment4_incentive_rate_gbpm]
    ,[isnumeric_outp_payment1_incentive_rate_gbpm]
    ,[isnumeric_outp_payment2_incentive_rate_gbpm]
    ,[onlynumeric_underp_payment1_incentive_rate_gbpm]
    ,[onlynumeric_underp_payment2_incentive_rate_gbpm]
    ,[onlynumeric_underp_payment3_incentive_rate_gbpm]
    ,[onlynumeric_underp_payment4_incentive_rate_gbpm]
    ,[onlynumeric_outp_payment1_incentive_rate_gbpm]
    ,[onlynumeric_outp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [isnumeric_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_underp_payment_incentive_enhanced_underp_payment]
      ,CAST(NULL as varchar(max)) as [onlynumeric_underp_payment_incentive_enhanced_underp_payment]
      ,CAST(NULL as varchar(max)) as [isnumeric_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [onlynumeric_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
      ,CAST(NULL as varchar(max)) as [isnumeric_outp_payment_incentive_enhanced_outp_payment]
      ,CAST(NULL as varchar(max)) as [onlynumeric_outp_payment_incentive_enhanced_outp_payment]
    ,[price_control_allocation_water_resources]
    ,[price_control_allocation_water_network_plus]
    ,[price_control_allocation_wastewater_network_plus]
    ,[price_control_allocation_bioresources_sludge]
    ,[price_control_allocation_residential_retail]
    ,[price_control_allocation_business_retail]
    ,[price_control_allocation_direct_procurement_for_customers]
    ,[price_control_allocation_dummy_control]
    from pr14 
        left join pc
        on ltrim(right(pr14.performance_commitment, len(pr14.performance_commitment) - charindex(':',pr14.performance_commitment)))=pc.pc_name
        and pr14.pc_unit=pc.pc_unit
        and pr14.pc_unit_description=pc.pc_unit_description
        and pr14.decimal_places=pc.decimal_places
        and pr14.primary_category=pc.primary_category
        left join company on pr14.company=company.water_company_acronym
        cross join amp
        where amp.amp_name = 'AMP6'
)

select * from final