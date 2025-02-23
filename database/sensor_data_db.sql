
CREATE DATABASE sensor_data;

CREATE TABLE sensor_temperatures (
    sensor_id INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    PRIMARY KEY (sensor_id, window_start, window_end) 
);