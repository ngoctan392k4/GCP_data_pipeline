{{ config(
    schema='raw_glamira_analysis',
    alias='dim_metal'
) }}

WITH metal_source AS (
  SELECT *
  FROM {{ref("stg_dim_metal")}}
)

SELECT *
FROM metal_source