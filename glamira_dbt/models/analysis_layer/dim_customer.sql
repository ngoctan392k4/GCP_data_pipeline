{{ config(
    schema='raw_glamira_analysis',
    alias='dim_customer'
) }}

WITH customer_source AS (
  SELECT *
  FROM {{ref("stg_dim_customer")}}
)

SELECT *
FROM customer_source