-- 날씨 원본 데이터
CREATE TABLE IF NOT EXISTS weather_raw (
    id           SERIAL PRIMARY KEY,
    city         VARCHAR(100) NOT NULL,
    temp_celsius NUMERIC(5,2),
    humidity     INTEGER,
    wind_speed   NUMERIC(5,2),
    rain_type    INTEGER,        -- 0=없음, 1=비, 2=비/눈, 3=눈
    rain_1h      NUMERIC(5,2),   -- 1시간 강수량 (mm)
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 일간 통계
CREATE TABLE IF NOT EXISTS weather_daily_stats (
    id           SERIAL PRIMARY KEY,
    city         VARCHAR(100) NOT NULL,
    stat_date    DATE NOT NULL,
    avg_temp     NUMERIC(5,2),
    max_temp     NUMERIC(5,2),
    min_temp     NUMERIC(5,2),
    avg_humidity INTEGER,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, stat_date)
);