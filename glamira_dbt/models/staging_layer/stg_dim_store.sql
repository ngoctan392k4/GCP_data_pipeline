WITH dim_store_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
)

SELECT DISTINCT
    dss.store_id,
    'Store ' || dss.store_id as store_name
FROM dim_store_source dss
