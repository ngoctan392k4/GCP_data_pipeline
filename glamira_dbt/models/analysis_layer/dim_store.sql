{{ config(
    schema='raw_glamira_analysis',
    alias='dim_store'
) }}

WITH store_source AS (
  SELECT *
  FROM {{ref("stg_dim_store")}}
)

SELECT *
FROM store_source