INSERT INTO
    daily_weather_summary (
        date,
        location,
        temp_max,
        temp_min,
        temp_avg,
        temp_range,
        processed_at
    )
SELECT
    date,
    location,
    temp_max,
    temp_min,
    (temp_max + temp_min) / 2 AS temp_avg,
    temp_max - temp_min AS temp_range,
    CURRENT_TIMESTAMP AS processed_at
FROM raw_weather
WHERE
    date NOT IN (
        SELECT date
        FROM daily_weather_summary
        WHERE
            location = raw_weather.location
    )
ON CONFLICT (date, location) DO NOTHING;