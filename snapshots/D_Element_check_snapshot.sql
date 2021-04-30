{% snapshot D_Element_check_snapshot %}

    {{
        config(
          target_schema='dw',
          strategy='check',
          unique_key='element_id',
          check_cols=['element_name','element_acronym','load_date'],
        )
    }}

   select  element_id
    , element_name
    , element_acronym
    ,load_date
    from {{ ref('D_Element')}}

{% endsnapshot %}