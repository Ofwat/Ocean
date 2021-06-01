with pr19 as (
    select * from {{ source('nw', 'PR19FinalCSVcreatedbyPython') }}
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
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.performance_commitment','pc.primary_category']))}} pc_company_amp_id
    ,pc.performance_commitment_id
    ,pc.performance_commitment
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
        left join pc on pr19.performance_commitment=pc.performance_commitment
        and pr19.pc_unit=pc.pc_unit
        /* few re cords are not getting in the final table as the desc in few is null or any other feild in the on query is null, so they dont get equal and we end up with no match
        eg     select 
    convert(varchar(50), hashbytes('md5', concat(unique_id, pc.performance_commitment, pc.primary_category, '')), 2)
 pc_company_amp_id
    ,pc.performance_commitment_id
    ,pc.performance_commitment
   ,company.water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    from pr19
        left join pc on pr19.performance_commitment=pc.performance_commitment
         and pr19.pc_unit=pc.pc_unit
        and pr19.pc_unit_description=pc.pc_unit_description for this eg u can see in the performace commitment table the unit desc is null so it didnt match
        and pr19.decimal_places=pc.decimal_places
       and pr19.primary_category=pc.primary_category
        left join company on pr19.company=company.water_company_acronym
        where  pc.[performance_commitment_id] = '57A967ECBE9B9938F75BE01D8F9DA2CC' */
        and pr19.pc_unit_description=pc.pc_unit_description
        and pr19.decimal_places=pc.decimal_places
        and pr19.primary_category=pc.primary_category
        left join company on pr19.company=company.water_company_acronym
        cross join amp
        where amp.amp_name = 'AMP7'
)

select * from final