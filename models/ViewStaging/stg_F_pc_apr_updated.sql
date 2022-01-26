    with amp6amp7base as (
        select * from {{ ref('stg_F_pc_apr_union') }}
    ),
    updates as (
        select * from {{ ref('pc_updates_snapshot') }}
        where dbt_valid_to is NULL
    ),
	actuals_updates as (
        SELECT updates.[unique_id]
            ,updates.OFWAT_Year AS year
            ,updates.submission_status as submission_status
            ,amp6amp7base.[price_review]
            ,amp6amp7base.[company_type]
            ,updates.company_acronym as company
            ,amp6amp7base.[element_name]
            ,amp6amp7base.[element_acronym]
            ,amp6amp7base.[outcome]
            ,amp6amp7base.[pc_ref]
            ,amp6amp7base.[annex]
            ,amp6amp7base.[performance_commitment]
            ,amp6amp7base.[odi_type]
            ,amp6amp7base.[odi_form]
            ,amp6amp7base.[odi_timing]
            ,amp6amp7base.[in_period_odi]
            ,amp6amp7base.[vanilla_odi]
            ,amp6amp7base.[primary_category]
            ,amp6amp7base.[pc_unit]
            ,amp6amp7base.[pc_unit_description]
            ,amp6amp7base.[decimal_places]
            ,amp6amp7base.[direction_of_improving_performance]
            ,amp6amp7base.[notional_outperformance_payment_or_underperformance_payment_accrued]
            ,amp6amp7base.[notional_outperformance_payment_or_underperformance_payment_accrued_GBPm]
            ,amp6amp7base.[outperformance_payment_or_underperformance_payment_in_period_ODI]
            ,updates.outperformance_or_underperformance_payment AS outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
            ,null as pcl
            ,updates.pcl_met AS pcl_met
            ,updates.pcl as performance_level_actual
            ,amp6amp7base.Total_AMP6_outperformance_payment_or_underperformance_payment_forecast as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
            ,updates.forecast_of_total_2020_25_outperformance_or_underperformance_payment AS Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
            ,amp6amp7base.[financial_odi]
            ,amp6amp7base.[underp_payment_collar]
            ,amp6amp7base.[notes_underp_payment_collar]
            ,amp6amp7base.[underp_payment_deadband]
            ,amp6amp7base.[outp_payment_deadband]
            ,amp6amp7base.[outp_payment_cap]
            ,amp6amp7base.[enhanced_underp_payment_collar]
            ,amp6amp7base.[standard_underp_payment_collar]
            ,amp6amp7base.[standard_outp_payment_cap]
            ,amp6amp7base.[enhanced_outp_payment_cap]
        FROM amp6amp7base 
        inner join updates 
		on updates.unique_id = amp6amp7base.unique_id
        and updates.OFWAT_Year = amp6amp7base.year
        and updates.company_acronym = amp6amp7base.company
		and updates.submission_status = 'Actual'
    ),
	past_performance_updates as (
        SELECT updates.[unique_id]
            ,updates.OFWAT_Year AS year
            ,updates.submission_status as submission_status
            ,amp6amp7base.[price_review]
            ,amp6amp7base.[company_type]
            ,updates.company_acronym as company
            ,amp6amp7base.[element_name]
            ,amp6amp7base.[element_acronym]
            ,amp6amp7base.[outcome]
            ,amp6amp7base.[pc_ref]
            ,amp6amp7base.[annex]
            ,amp6amp7base.[performance_commitment]
            ,amp6amp7base.[odi_type]
            ,amp6amp7base.[odi_form]
            ,amp6amp7base.[odi_timing]
            ,amp6amp7base.[in_period_odi]
            ,amp6amp7base.[vanilla_odi]
            ,amp6amp7base.[primary_category]
            ,amp6amp7base.[pc_unit]
            ,amp6amp7base.[pc_unit_description]
            ,amp6amp7base.[decimal_places]
            ,amp6amp7base.[direction_of_improving_performance]
            ,null as notional_outperformance_payment_or_underperformance_payment_accrued
            ,null as notional_outperformance_payment_or_underperformance_payment_accrued_GBPm
            ,null as outperformance_payment_or_underperformance_payment_in_period_ODI
            ,null as outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
            ,null as pcl
            ,updates.pcl_met as pcl_met
            ,updates.pcl as performance_level_actual
            ,null as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast
            ,null as Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm
            ,null as financial_odi
            ,null as underp_payment_collar
            ,null as notes_underp_payment_collar
            ,null as underp_payment_deadband
            ,null as outp_payment_deadband
            ,null as outp_payment_cap
            ,null as enhanced_underp_payment_collar
            ,null as standard_underp_payment_collar
            ,null as standard_outp_payment_cap
            ,null as enhanced_outp_payment_cap
        FROM amp6amp7base 
        inner join updates 
		on updates.unique_id = amp6amp7base.unique_id
        and updates.company_acronym = amp6amp7base.company
		and updates.submission_status = 'Past performance'
    ),
    renamed as (
        select * from amp6amp7base
		UNION
		select * from actuals_updates
		UNION
		select * from past_performance_updates
    )

    select * from renamed
