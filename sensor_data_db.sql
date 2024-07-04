
CREATE DATABASE sensor_data;

CREATE TABLE sensor_temperatures (
    sensor_id INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    PRIMARY KEY (sensor_id, window_start, window_end)  -- Cambia el índice único a estas columnas
);

CREATE TABLE sensor_temperatures_hourly (
    sensor_id INT,
    record_date DATE,
    record_hour TIME,
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    PRIMARY KEY (sensor_id, record_date, record_hour)
);