{{
    config(
        materialized='incremental',
        unique_key='element_id'
    )
}}

    select element_id
    ,element_name
    ,element_acronym
    ,load_date
    from {{ ref('D_Element_Inc')}}
    
{% if is_incremental() %}
  where load_date >= (select max(load_date) from {{ this }})
{% endif %}
