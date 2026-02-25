WITH daily_weather as
(

SELECT date(time) as daily_weather,
weather,
temp,
pressure,
humidity,
clouds
FROM {{ source('demo', 'weather') }}
),

daily_weather_agg as (
select daily_weather,weather,
round(avg(temp),2) as avgtemp,
round(avg(pressure),2) as avgpressure,
round(avg(humidity),2) as avghumidity,
round(avg(clouds),2) as avgcloud
from daily_weather
group by daily_weather,weather
qualify ROW_NUMBER() OVER (PARTITION BY daily_weather ORDER BY count(weather) desc) =1
)

SELECT * FROM daily_weather_agg