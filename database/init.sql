CREATE TABLE IF NOT EXISTS raw_weather (
    date DATE,
    location TEXT,
    temp_max FLOAT,
    temp_min FLOAT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_weather_summary (
    date DATE,
    location TEXT,
    temp_max FLOAT,
    temp_min FLOAT,
    temp_avg FLOAT,
    temp_range FLOAT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, location)
);