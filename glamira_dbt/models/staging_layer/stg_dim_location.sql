WITH dim_location_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_ip_location')}}
)

SELECT DISTINCT
    FARM_FINGERPRINT(dls.country || dls.region || dls.city) AS location_id,
    dls.ip AS ip_address,
    dls.country AS country_name,
    dls.country_short AS country_short,
    dls.region AS region_name,
    dls.city AS city_name
FROM dim_location_source dls
