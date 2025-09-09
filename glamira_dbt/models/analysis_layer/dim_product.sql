{{ config(
    schema='glamira_analysis',
    alias='dim_product',
    materialized='table'
) }}

WITH product_source AS (
  SELECT *
  FROM {{ref("stg_dim_product")}}
)


SELECT
  ps.product_id,
  ps.product_name,
  ps.sku,
  ps.attribute_set_id,
  ps.type_id,
  ps.min_price,
  ps.max_price,
  ps.collection_id,
  ps.product_type_value,
  ps.category as product_subtype_id,
  ps.store_code,
  ps.gender
FROM product_source ps

-- FROM english_product_name

-- UNION ALL
-- SELECT data.product_id, data.product_name, data.sku, data.attribute_set_id, data.type_id, data.min_price, data.max_price, data.collection_id, data.product_type_value, data.category as product_subtype_id, data.store_code, data.gender
-- FROM other_latin_product_name

-- UNION ALL
-- SELECT data.product_id, data.product_name, data.sku, data.attribute_set_id, data.type_id, data.min_price, data.max_price, data.collection_id, data.product_type_value, data.category as product_subtype_id, data.store_code, data.gender
-- FROM remaining_picked as data
