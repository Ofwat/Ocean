with pr14 as (
    select * from {{ source('generated_sources', 'PR14BaseCSVcreatedbyPython') }}
),
company as (
    select * from {{ ref('D_Water_company_table') }}
),
pc as (
    select * from {{ ref('stg_PR14_Performance_commitment') }}
),
element as (
    select * from {{ ref('D_Element_table') }}
),

pr as (
    select * from {{ ref('D_Price_review_table') }}
),
odi_characteristics as (
    select * from {{ ref('stg_PR14_ODI_characteristics') }}
),
final as (
    select {{dbt_utils.hash(dbt_utils.concat(['unique_id','pc.performance_commitment','pc.primary_category']))}} pc_company_pr_id
    ,'PC list' sheet
    ,pr.price_review
    ,pc.performance_commitment_id
    ,pc.performance_commitment
    ,CAST(NULL as varchar(max)) as  pc_short_description
    ,company.water_company_id
    ,unique_id
    ,outcome
    ,PC_ref
    ,odi_characteristics.ODI_characteristics_id
    ,CAST(NULL as varchar(max)) as common_comparable_bespoke_performance_commitment
    ,annex
    ,direction_of_improving_performance
    ,PR14_comparative_drinking_water_compliance
    ,PR14_comparative_water_quality_contacts
    ,PR14_comparative_supply_interruptions_3_hours
    ,PR14_comparative_pollution_incidents_cat_3
    ,PR14_comparative_internal_sewer_flooding
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
    ,notes_underp_payment1_incentive_rate_gbpm
    ,underp_payment1_incentive_rate_gbpm
    ,notes_underp_payment2_incentive_rate_gbpm
    ,underp_payment2_incentive_rate_gbpm
    ,notes_underp_payment3_incentive_rate_gbpm
    ,underp_payment3_incentive_rate_gbpm
    ,notes_underp_payment4_incentive_rate_gbpm
    ,underp_payment4_incentive_rate_gbpm
    ,notes_outp_payment1_incentive_rate_gbpm
    ,outp_payment1_incentive_rate_gbpm
    ,notes_outp_payment2_incentive_rate_gbpm
    ,outp_payment2_incentive_rate_gbpm
    ,CAST(NULL as varchar(max)) as [notes_underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_underp_payment_incentive_enhanced_underp_payment]
    ,CAST(NULL as varchar(max)) as [underp_payment_incentive_enhanced_underp_payment]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
    ,CAST(NULL as varchar(max)) as [notes_outp_payment_incentive_enhanced_outp_payment]
    ,CAST(NULL as varchar(max)) as [outp_payment_incentive_enhanced_outp_payment]
    ,price_control_allocation_water_resources
    ,price_control_allocation_water_network_plus
    ,price_control_allocation_wastewater_network_plus
    ,price_control_allocation_bioresources_sludge
    ,price_control_allocation_residential_retail
    ,price_control_allocation_business_retail
    ,price_control_allocation_direct_procurement_for_customers
    ,price_control_allocation_dummy_control
    from pr14 
        left join pc
        on isnull(ltrim(right(pr14.performance_commitment, len(pr14.performance_commitment) - charindex(':',pr14.performance_commitment))),'performance_commitment')=isnull(pc.performance_commitment,'performance_commitment')
        and isnull(pr14.pc_unit,'pc_unit') = isnull(pc.pc_unit,'pc_unit')
        and isnull(pr14.pc_unit_description,'pc_unit_description') = isnull(pc.pc_unit_description,'pc_unit_description')
        and isnull(pr14.decimal_places,'decimal_places') = isnull(pc.decimal_places,'decimal_places')
        and isnull(pr14.primary_category,'primary_category') = isnull(pc.primary_category,'primary_category')
        left join company on isnull(pr14.company,'company') = isnull(company.water_company_acronym,'company')
        cross join pr
    left join odi_characteristics on isnull(pr14.odi_form,'odi_form') = isnull(odi_characteristics.odi_form,'odi_form')
            and isnull(pr14.odi_type,'odi_type') = isnull(odi_characteristics.odi_type,'odi_type')
        where pr.price_review = 'PR14' and unique_id is not null
)

select * from final