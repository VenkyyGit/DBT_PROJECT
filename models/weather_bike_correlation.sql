WITH weather_bike as (

    SELECT
    t.*,
    w.*
    FROM {{ ref('trip_fact') }} t
    LEFT JOIN {{ ref('daily_weather') }} w
    ON t.trip_date=w.daily_weather
)

SELECT * from weather_bike