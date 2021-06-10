    with amp6amp7base as (
        select * from {{ ref('stg_F_pc_apr_union') }}
    ),
    updates as (
        select * from {{ ref('pc_updates_snapshot') }}
        where dbt_valid_to is NULL
    ),
    renamed as (
        SELECT amp6amp7base.[unique_id]
            ,[year]
            ,CASE 
                WHEN updates.unique_id IS NULL THEN amp6amp7base.submission_status
                ELSE updates.submission_status
            END AS submission_status
            ,[amp_id]
            ,[company_type]
            ,[company]
            ,[element_name]
            ,[element_acronym]
            ,[outcome]
            ,[pc_ref]
            ,[annex]
            ,[performance_commitment]
            ,[odi_type]
            ,[odi_form]
            ,[odi_timing]
            ,[in_period_odi]
            ,[vanilla_odi]
            ,[primary_category]
            ,[pc_unit]
            ,[pc_unit_description]
            ,[decimal_places]
            ,[direction_of_improving_performance]
            ,[notional_outperformance_payment_or_underperformance_payment_accrued]
            ,[notional_outperformance_payment_or_underperformance_payment_accrued_GBPm]
            ,[outperformance_payment_or_underperformance_payment_in_period_ODI]
            -- ,[outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm]
            ,CASE 
                 WHEN updates.unique_id IS NULL THEN amp6amp7base.[outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm]
                 ELSE updates.outperformance_or_underperformance_payment
            END AS outperformance_payment_or_underperformance_payment_in_period_ODI_GBPm
            ,CASE 
                WHEN updates.unique_id IS NULL THEN amp6amp7base.pcl
                ELSE updates.pcl
            END AS pcl
            ,CASE 
                WHEN updates.unique_id IS NULL THEN amp6amp7base.pcl_met
                ELSE updates.pcl_met
            END AS pcl_met
            ,[performance_level_actual]
            ,[Total_AMP6_outperformance_payment_or_underperformance_payment_forecast] as Total_AMP_outperformance_payment_or_underperformance_payment_forecast
            -- ,[Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm]
            ,CASE 
                 WHEN updates.unique_id IS NULL THEN amp6amp7base.[Total_AMP6_outperformance_payment_or_underperformance_payment_forecast_GBPm]
                 ELSE updates.forecast_of_total_2020_25_outperformance_or_underperformance_payment
            END AS Total_AMP_outperformance_payment_or_underperformance_payment_forecast_GBPm
            ,[financial_odi]
            ,[underp_payment_collar]
            ,[underp_payment_deadband]
            ,[outp_payment_deadband]
            ,[outp_payment_cap]
            ,[enhanced_underp_payment_collar]
            ,[standard_underp_payment_collar]
            ,[standard_outp_payment_cap]
            ,[enhanced_outp_payment_cap]
        FROM amp6amp7base 
        full outer join updates 
        on updates.unique_id = amp6amp7base.unique_id
        and updates.OFWAT_Year = amp6amp7base.year
        and updates.company_acronym = amp6amp7base.company
        -- where amp6amp7base.Year = '2020-21'
        --and ae_staging.stg_F_pc_apr_union.company = 'UU' 
        -- and amp6amp7base.unique_id in ('PR19TMS_BW04', 'PR19UUW_B01-WN')
    )

    select * from renamed
