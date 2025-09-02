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


SELECT data.*
FROM english_product_name

UNION ALL
SELECT data.*
FROM other_latin_product_name

UNION ALL
SELECT *
FROM remaining_picked
