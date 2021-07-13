select * from {{ ref('stg_PR14_ODI_characteristics') }}
union 
select * from {{ ref('stg_PR19_ODI_characteristics_App1') }}
union 
select * from {{ ref('stg_PR19_ODI_characteristics_App1b') }}
