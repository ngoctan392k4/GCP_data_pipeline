{{ config(
    materialized='table'
) }}

WITH english_product_name AS (
  SELECT
      product_id,
      ARRAY_AGG(rpd ORDER BY min_price LIMIT 1)[OFFSET(0)] AS data
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.min_price LIKE '$%'
  GROUP BY product_id
),

other_latin_product_name AS (
  SELECT
      product_id,
      ARRAY_AGG(rpd ORDER BY product_name LIMIT 1)[OFFSET(0)] AS data
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.product_id NOT IN (SELECT product_id FROM english_product_name)
    AND REGEXP_CONTAINS(rpd.product_name, r'^[A-Za-z0-9\s\-\.,]+$')
  GROUP BY product_id
),

remaining_products AS (
  SELECT *
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.product_id NOT IN (SELECT product_id FROM english_product_name)
    AND rpd.product_id NOT IN (SELECT product_id FROM other_latin_product_name)
),

random_suffix AS (
  SELECT
      product_id,
      ARRAY_AGG(suffix ORDER BY suffix LIMIT 1)[OFFSET(0)] AS picked_suffix
  FROM remaining_products
  GROUP BY product_id
),

remaining_picked AS (
  SELECT rpd.*
  FROM remaining_products rpd
  JOIN random_suffix ps
    ON rpd.product_id = ps.product_id
   AND rpd.suffix = ps.picked_suffix
)


SELECT
  data.product_id,
  data.suffix,
  data.product_name,
  data.sku,
  data.attribute_set_id,
  data.type_id,
  data.min_price,
  data.max_price,
  COALESCE(NULLIF(data.collection_id, ''), 'Not Defined') AS collection_id,
  -- CASE
  --   WHEN data.collection_id = '-' THEN 'Not Defined'
  --   ELSE data.collection_id
  -- END AS collection_id,
  data.product_type_value,
  data.category,
  data.store_code,
  data.gender,
  data.stone,
  data.color,
  data.alloy
FROM english_product_name

UNION ALL
SELECT
  data.product_id,
  data.suffix,
  data.product_name,
  data.sku,
  data.attribute_set_id,
  data.type_id,
  data.min_price,
  data.max_price,
  COALESCE(NULLIF(data.collection_id, ''), 'Not Defined') AS collection_id,
  -- CASE
  --   WHEN data.collection_id = '-' THEN 'Not Defined'
  --   ELSE data.collection_id
  -- END AS collection_id,
  data.product_type_value,
  data.category,
  data.store_code,
  data.gender,
  data.stone,
  data.color,
  data.alloy
FROM other_latin_product_name

UNION ALL
SELECT
  rp.product_id,
  rp.suffix,
  rp.product_name,
  rp.sku,
  rp.attribute_set_id,
  rp.type_id,
  rp.min_price,
  rp.max_price,
  COALESCE(NULLIF(rp.collection_id, ''), 'Not Defined') AS collection_id,
  -- CASE
  --   WHEN rp.collection_id = '-' THEN 'Not Defined'
  --   ELSE rp.collection_id
  -- END AS collection_id,
  rp.product_type_value,
  rp.category,
  rp.store_code,
  rp.gender,
  rp.stone,
  rp.color,
  rp.alloy
FROM remaining_picked rp


