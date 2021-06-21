with pr19 as (
    select * from {{ source('nw', 'PR19FinalCSVcreatedbyPython') }}
),
company as (
    select * from {{ ref('D_Water_company_table') }}
),
pc as (
    select * from {{ ref('stg_PR19_Performance_commitment') }}
),
pr as (
    select * from {{ ref('D_Price_review_table') }}
),

final as (
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.performance_commitment','pc.primary_category']))}} pc_company_pr_id
    ,pr.price_review
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
    ,CAST(NULL as varchar(max)) as supply_interruptions_3_hours
    ,CAST(NULL as varchar(max)) as pollution_incidents_cat_3
    ,CAST(NULL as varchar(max)) as internal_sewer_flooding
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
        left join pc on isnull(pr19.performance_commitment,'performance_commitment') = isnull(pc.performance_commitment,'performance_commitment')
        and isnull(pr19.pc_unit,'pc_unit') = isnull(pc.pc_unit,'pc_unit')
        and isnull(pr19.pc_unit_description,'pc_unit_description') = isnull(pc.pc_unit_description,'pc_unit_description')
        and isnull(pr19.decimal_places,'decimal_places') = isnull(pc.decimal_places,'decimal_places')
        and isnull(pr19.primary_category,'primary_category') = isnull(pc.primary_category,'primary_category')
        left join company on isnull(pr19.company,'company') = isnull(company.water_company_acronym,'company')
        cross join pr
        where pr.price_review = 'PR19' and unique_id is not null
)

select * from final