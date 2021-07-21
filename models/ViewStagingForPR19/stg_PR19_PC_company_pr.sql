with pr19 as (
    select * from {{ source('generated_sources', 'PR19FinalCSVcreatedbyPython') }}
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
odi_characteristics as (
    select * from {{ ref('stg_PR19_ODI_characteristics') }}
),

final as (
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.performance_commitment','pc.primary_category']))}} pc_company_pr_id
    ,pr.price_review
    ,pc.performance_commitment_id
    ,pc.performance_commitment
    ,pc.pc_short_description
    ,company.water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    ,odi_characteristics.ODI_characteristics_id
    ,common_and_comparable_bespoke_performance_commitment as common_comparable_bespoke_performance_commitment
    ,CAST(NULL as varchar(max)) as annex
    ,direction_of_improving_performance
    ,CAST(NULL as varchar(max)) as PR14_comparative_drinking_water_compliance
    ,CAST(NULL as varchar(max)) as PR14_comparative_water_quality_contacts
    ,CAST(NULL as varchar(max)) as PR14_comparative_supply_interruptions_3_hours
    ,CAST(NULL as varchar(max)) as PR14_comparative_pollution_incidents_cat_3
    ,CAST(NULL as varchar(max)) as PR14_comparative_internal_sewer_flooding
    ,CAST(NULL as varchar(max)) as special_cost_factor
    ,CAST(NULL as varchar(max)) as scheme_specific_factor
    ,CAST(NULL as varchar(max)) as asset_health
    ,CAST(NULL as varchar(max)) as nep
    ,CAST(NULL as varchar(max)) as AIM
    ,customers_relative_priority
    ,CAST(NULL as varchar(max)) as no_of_sub_measures
    ,standard_odi_cal
    ,standard_odi_operand
    ,standard_odi_operand_note
    ,CAST(NULL as varchar(max)) as [notes_underp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [underp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [underp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment3_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [underp_payment3_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment4_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [underp_payment4_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [outp_payment1_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment2_incentive_rate_gbpm]
    ,CAST(NULL as varchar(max)) as [outp_payment2_incentive_rate_gbpm]
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
        left join pc on isnull(pr19.performance_commitment,'performance_commitment') = isnull(pc.performance_commitment,'performance_commitment')
        and isnull(pr19.pc_unit,'pc_unit') = isnull(pc.pc_unit,'pc_unit')
        and isnull(pr19.pc_unit_description,'pc_unit_description') = isnull(pc.pc_unit_description,'pc_unit_description')
        and isnull(pr19.decimal_places,'decimal_places') = isnull(pc.decimal_places,'decimal_places')
        and isnull(pr19.primary_category,'primary_category') = isnull(pc.primary_category,'primary_category')
        and isnull(pr19.pc_short_description,'pc_short_description') = isnull(pc.pc_short_description,'pc_short_description')
        left join company on isnull(pr19.company,'company') = isnull(company.water_company_acronym,'company')
        cross join pr
        left join odi_characteristics on isnull(pr19.odi_form,'odi_form') = isnull(odi_characteristics.odi_form,'odi_form')
            and isnull(pr19.odi_type,'odi_type') = isnull(odi_characteristics.odi_type,'odi_type')
            and isnull(pr19.odi_timing,'odi_timing') = isnull(odi_characteristics.odi_timing,'odi_timing')
        where pr.price_review = 'PR19' and unique_id is not null
)

select * from final