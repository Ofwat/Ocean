select {{dbt_utils.hash(dbt_utils.concat(['price_review','price_review_description']))}} price_review_id
    ,price_review
    ,price_review_description
    ,price_base
    from {{ ref('raw_Price_review') }}

