select * from {{ ref('stg_PR14_PC_company_pr') }}
union 
select * from {{ ref('stg_PR19_PC_company_pr') }}
