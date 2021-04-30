{{
    config(
        materialized='incremental',
        unique_key='element_id'
    )
}}

    select  {{dbt_utils.hash(dbt_utils.concat(['element_acronym','element_name']))}} element_id
    ,element_name
    ,element_acronym
    ,load_date
    from (select distinct element_name, element_acronym, load_date from {{ ref('PR14FinalCSVcreatedbyPythonView')}}) e
  
{% if is_incremental() %}
  where load_date >= (select max(load_date) from {{ this }})
{% endif %}
