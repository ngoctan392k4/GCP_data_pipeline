WITH fact_sale_source AS (
    SELECT *
    FROM {{ source('glamira_src', 'raw_glamira_behaviour') }}
    WHERE collection = 'checkout_success'
),

product_source AS (
    SELECT *
    FROM {{ ref("stg_dim_product") }} as ps

),

fact_sales as (
    SELECT DISTINCT
        FARM_FINGERPRINT(fsc.order_id || '-' || cp.product_id) AS sale_id,
        fsc.order_id,
        cp.product_id,
        FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP_SECONDS(fsc.time_stamp))) AS date_id,
        fsc.store_id,

		-- Dù alloy hay stone thì đều có option_id và option_type_id nên cần check hash xong thì thuộc bên nào

		MAX(
            CASE
                WHEN FARM_FINGERPRINT(opt.option_id || '-' || opt.value_id)
                     IN (SELECT stone_id FROM {{ ref("stg_dim_stone") }})
                THEN FARM_FINGERPRINT(opt.option_id || '-' || opt.value_id)
            END
        ) AS stone_id,

        MAX(
            CASE
                WHEN psc.option_id IS NOT NULL
                    AND FARM_FINGERPRINT(psc.colour) IN (SELECT color_id FROM {{ ref("stg_dim_color") }})
                THEN FARM_FINGERPRINT(psc.colour)
                WHEN psa.option_id IS NOT NULL
                    AND FARM_FINGERPRINT(psa.colour) IN (SELECT color_id FROM {{ ref("stg_dim_color") }})
                THEN FARM_FINGERPRINT(psa.colour)
            END
        ) AS color_id,

        MAX(
            CASE
                WHEN psc.option_id IS NOT NULL
                    AND FARM_FINGERPRINT(psc.metal) IN (SELECT metal_id FROM {{ ref("stg_dim_metal") }})
                THEN FARM_FINGERPRINT(psc.metal)
                WHEN psa.option_id IS NOT NULL
                    AND FARM_FINGERPRINT(psa.metal) IN (SELECT metal_id FROM {{ ref("stg_dim_metal") }})
                THEN FARM_FINGERPRINT(psa.metal)
            END
        ) AS metal_id,

        fsc.device_id as customer_id,
        fsc.ip AS ip_address,
        fsc.local_time,
        MAX(cp.amount) AS quantity,
		-- MAX(CAST(REPLACE(REPLACE(REPLACE(REPLACE(cp.price, '\\', ''), '"', ''), "'", ''), ',', '.') AS FLOAT64)) AS price,
        MAX(SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cp.price, '.', ''), ',', '.'), '\\', ''), '"', ''), "'", '') AS FLOAT64)) AS price,
        cp.currency,
        ANY_VALUE(CAST(exc.exchange_rate AS FLOAT64)) AS exchange_rate_to_usd,
		-- MAX(cp.amount*CAST(REPLACE(REPLACE(REPLACE(REPLACE(cp.price, '\\', ''), '"', ''), "'", ''), ',', '.') AS FLOAT64)*CAST(exc.exchange_rate AS FLOAT64)) AS total_in_usd
        -- MAX(cp.amount * SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cp.price, '.', ''), ',', '.'), '\\', ''), '"', ''), "'", '') AS FLOAT64) * ANY_VALUE(CAST(exc.exchange_rate AS FLOAT64))) AS total_in_usd
    FROM fact_sale_source AS fsc
	CROSS JOIN UNNEST(fsc.cart_products) AS cp
    CROSS JOIN UNNEST(cp.option) AS opt
    JOIN product_source AS ps
		ON ps.product_id = CAST(cp.product_id AS STRING)
	LEFT JOIN UNNEST(ps.stone) pst
		ON pst.option_id = CAST(opt.option_id AS STRING)
		AND pst.option_type_id = CAST(opt.value_id AS STRING)
	LEFT JOIN UNNEST(ps.color) psc
		ON psc.option_id = CAST(opt.option_id AS STRING)
		AND psc.option_type_id = CAST(opt.value_id AS STRING)
	LEFT JOIN UNNEST(ps.alloy) psa
		ON psa.option_id = CAST(opt.option_id AS STRING)
		AND psa.option_type_id = CAST(opt.value_id AS STRING)
    LEFT JOIN {{ ref('exchange_rate') }} exc
		ON cp.currency = exc.symbol
	GROUP BY fsc.order_id, cp.product_id, date_id, fsc.store_id, fsc.device_id, fsc.local_time, cp.currency, fsc.ip
)


SELECT *
FROM fact_sales
