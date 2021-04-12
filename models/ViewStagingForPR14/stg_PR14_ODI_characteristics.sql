SELECT distinct(odi_type)
      ,odi_form
      ,CAST(NULL as varchar(max)) as odi_timing
  FROM {{ ref('PR14FinalCSVcreatedbyPythonView') }}