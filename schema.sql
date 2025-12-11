CREATE TABLE IF NOT EXISTS weather (
    weather_id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    weather_description TEXT,
    precipitation DOUBLE PRECISION,
    precipitation_type TEXT,
    temp_max DOUBLE PRECISION,
    temp_min DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS accidents (
    accident_id BIGINT PRIMARY KEY,
    crash_datetime TIMESTAMP NOT NULL,
    crash_date DATE NOT NULL,
    borough TEXT,
    zip_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    street_name TEXT,
    street_type TEXT,
    num_injuries INT,
    num_deaths INT,
    severity INT,  
    weather_id INT REFERENCES weather(weather_id)
);
CREATE INDEX IF NOT EXISTS idx_accidents_crash_datetime
    ON accidents(crash_datetime);

CREATE INDEX IF NOT EXISTS idx_accidents_severity
    ON accidents(severity);

CREATE INDEX IF NOT EXISTS idx_accidents_lat_lon
    ON accidents(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_accidents_borough_date
    ON accidents(borough, crash_date);