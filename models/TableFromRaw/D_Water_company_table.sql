select DISTINCT {{dbt_utils.hash(dbt_utils.concat(['water_company_acronym','water_company_name']))}} as water_company_id
,water_company_acronym
,water_company_name
,water_company_type
,water_company_type_description 
from {{ ref('raw_Water_company') }}