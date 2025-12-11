CREATE DATABASE IF NOT EXISTS weather_db;

USE weather_db;

CREATE TABLE IF NOT EXISTS weather_db.weather_hourly (
    city String,
    timestamp DateTime,
    temperature Float32,
    precipitation Float32,
    wind_speed Float32,
    wind_direction Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (city, timestamp);

CREATE TABLE IF NOT EXISTS weather_db.weather_daily (
    city String,
    date Date,
    temp_min Float32,
    temp_max Float32,
    temp_avg Float32,
    total_precipitation Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (city, date);