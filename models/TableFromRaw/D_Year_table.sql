select  {{dbt_utils.hash(dbt_utils.concat(['year']))}} year_id,amp_name,year,start_date,end_date
 from {{ ref('raw_Year') }}

