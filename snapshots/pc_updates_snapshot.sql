{% snapshot pc_updates_snapshot %}

{{
    config(
      target_schema=generate_schema_name('snapshots'),
      unique_key='pc_updates_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

    SELECT * from {{ ref('stg_pc_updates')}}

{% endsnapshot %}


