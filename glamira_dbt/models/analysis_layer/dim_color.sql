{{ config(
    schema='raw_glamira_analysis',
    alias='dim_color'
) }}

WITH color_source AS (
  SELECT *
  FROM {{ref("stg_dim_color")}}
)

SELECT *
FROM color_source