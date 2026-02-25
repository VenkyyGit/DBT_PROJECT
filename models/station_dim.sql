WITH bike AS (
SELECT 
distinct
START_STATIO_ID as station_id,
start_station_name as station_name,
START_LAT as station_lat,
START_LNG as start_station_lng
FROM {{ source('demo', 'biketable') }}
WHERE RIDE_ID!='ride_id'
)

select * from bike