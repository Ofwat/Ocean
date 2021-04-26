    select  {{dbt_utils.hash(dbt_utils.concat(['element_acronym','element_name']))}} element_id
    ,element_name
    ,element_acronym
    ,load_date
    from (select distinct element_name, element_acronym from {{ ref('PR14FinalCSVcreatedbyPythonView')}}) e
    