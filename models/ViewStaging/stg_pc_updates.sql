    with source_update as (
        select * from {{ source('generated_sources', 'pc_actuals_updates') }}
    ),
    company as (
        select * from {{ ref('D_Water_company')}}
    ),
    renamed as (
        select {{dbt_utils.hash(dbt_utils.concat(['amp_name','OFWAT_Year','submission_status','unique_id','company_acronym']))}} pc_updates_id
        ,[updated_at]
        ,[amp_name]
        ,[OFWAT_Year]
        ,[submission_status]
        ,[unique_id]
        ,[company_acronym]
        ,company.water_company_name as [company_name]
        ,[pcl]
        ,[pcl_met]
        ,[outperformance_or_underperformance_payment]
        ,[forecast_of_total_2020_25_outperformance_or_underperformance_payment]
        from source_update
        inner join company 
        on source_update.company_acronym = company.water_company_acronym
    )

    select * from renamed


