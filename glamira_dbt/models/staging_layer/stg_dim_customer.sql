WITH dim_customer_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
)

SELECT DISTINCT
    dcs.device_id as customer_id,
    dcs.email_address,
    dcs.user_agent,
    dcs.user_id_db,
    dcs.resolution,
    dcs.utm_source,
    dcs.utm_medium
FROM dim_customer_source dcs