{% snapshot D_Element_timestamp_snapshot %}

    {{
        config(
          target_schema='dw',
          strategy='timestamp',
          unique_key='element_id',
          updated_at='load_date',
          check_cols=['element_name','element_acronym','load_date'],
        )
    }}

   select  element_id
    , element_name
    , element_acronym
    ,load_date
    from {{ ref('D_Element')}}

{% endsnapshot %}