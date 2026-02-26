-- 날씨 데이터 저장 테이블
CREATE TABLE IF NOT EXISTS weather_raw (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100) NOT NULL,
    temp_celsius NUMERIC(5,2),
    feels_like  NUMERIC(5,2),
    humidity    INTEGER,
    description VARCHAR(200),
    wind_speed  NUMERIC(5,2),
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 일간 통계 테이블
CREATE TABLE IF NOT EXISTS weather_daily_stats (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100) NOT NULL,
    stat_date   DATE NOT NULL,
    avg_temp    NUMERIC(5,2),
    max_temp    NUMERIC(5,2),
    min_temp    NUMERIC(5,2),
    avg_humidity INTEGER,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (city, stat_date)
);
