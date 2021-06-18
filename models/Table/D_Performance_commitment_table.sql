SELECT [performance_commitment_id]
      ,[performance_commitment]
      ,[primary_category]
      ,[pc_unit]
      ,[pc_unit_description]
      ,[decimal_places]
  FROM {{ref('stg_Performance_commitment_union') }}