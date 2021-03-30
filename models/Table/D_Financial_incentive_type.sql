select financial_incentive_id,
financial_incentive_type,Incentive_Period from {{ ref('stg_PR14_Financial_incentive_type') }}

