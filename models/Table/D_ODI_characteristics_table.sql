SELECT ODI_characteristics_id
,odi_type
,odi_form
,odi_timing
  FROM {{ ref('stg_ODI_characteristics') }}