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
    select * from {{ ref('stg_AMP_year') }}
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
    ,'N/A' common_comparable_bespoke_performance_commitment
    ,annex
    ,direction_of_improving_performance
    ,drinking_water_quality_compliance
    ,water_quality_contacts
    ,supply_interruptions_3_hours
    ,pollution_incidents_cat_3
    ,internal_sewer_flooding
    ,'N/A' special_cost_factor
    ,scheme_specific_factor
    ,asset_health
    ,nep
    ,AIM
    ,'N/A' customers_relative_priority
    ,no_of_sub_measures
    ,'N/A' standard_odi_cal
    ,standard_odi_operand
    ,standard_odi_operand_note
    ,[underp_payment1_incentive_rate_gbpm]
    ,[underp_payment2_incentive_rate_gbpm]
    ,[underp_payment3_incentive_rate_gbpm]
    ,[underp_payment4_incentive_rate_gbpm]
    ,[outp_payment1_incentive_rate_gbpm]
    ,[outp_payment2_incentive_rate_gbpm]
    ,'N/A' [underp_payment_incentive_standard_underp_payment1_tier2_where_tiers_apply]
    ,'N/A' [underp_payment_incentive_standard_underp_payment2_tier1_where_tiers_apply]
    ,'N/A' [underp_payment_incentive_standard_underp_payment3_tier3_where_tiers_apply]
    ,'N/A' [underp_payment_incentive_enhanced_underp_payment]
    ,'N/A' [outp_payment_incentive_standard_outp_payment1_tier2_where_tiers_apply]
    ,'N/A' [outp_payment_incentive_standard_outp_payment2_tier1_where_tiers_apply]
    ,'N/A' [outp_payment_incentive_standard_outp_payment3_tier3_where_tiers_apply]
    ,'N/A' [outp_payment_incentive_enhanced_outp_payment]
    ,water_resources
    ,water_network_plus
    ,wastewater_network_plus
    ,bioresources_sludge
    ,residential_retail
    ,business_retail
    ,direct_procurement_for_customers
    ,dummy_control
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