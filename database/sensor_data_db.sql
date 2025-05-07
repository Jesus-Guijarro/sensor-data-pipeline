
CREATE DATABASE sensor_data;

CREATE TABLE sensors (
    sensor_id   INTEGER PRIMARY KEY,
    city        VARCHAR(100) NOT NULL,
    station     VARCHAR(100) NOT NULL,
    diff_temperature  INTEGER NOT NULL
);

INSERT INTO sensors (sensor_id, city,    station,                              diff_temperature) VALUES
  ( 1, 'Valencia',  'Valencia Aeropuerto (Manises)',    -1),
  ( 2, 'Valencia',  'Valencia-Estación del Norte',      -1),
  ( 3, 'Valencia',  'Valencia-Albufera',                -1),
  ( 4, 'Valencia',  'Valencia-Marítimo (Malvarrosa)',   -1),

  ( 5, 'Murcia',    'Murcia Aeropuerto (Corvera)',      +1),
  ( 6, 'Murcia',    'Murcia-Estación del Carmen',       +1),
  ( 7, 'Murcia',    'Murcia-Santomera',                 +1),

  ( 8, 'Alicante',  'Alicante Aeropuerto (El Altet)',   0),
  ( 9, 'Alicante',  'Alicante-Estación Central',        0),
  (10, 'Alicante',  'Alicante-Universidad (UA)',        0),

  (11, 'Elche',     'Elche Aeropuerto (El Altet)',      -1),
  (12, 'Elche',     'Elche-Parque Empresarial',         -1),

  (13, 'Cartagena', 'Cartagena Puerto (Caladero)',      +2),
  (14, 'Cartagena', 'Cartagena-La Aljorra',             +2),

  (15, 'Almería',   'Almería Aeropuerto (LEI)',         +2),
  (16, 'Almería',   'Almería-Cabo de Gata',             +2)
;

CREATE TABLE sensor_temperatures (
    sensor_id     INT         NOT NULL,
    window_start  TIMESTAMP   NOT NULL,
    window_end    TIMESTAMP   NOT NULL,
    temperature   INTEGER,
    humidity      INTEGER,
    PRIMARY KEY (sensor_id, window_start, window_end),
    FOREIGN KEY (sensor_id)
        REFERENCES sensors(sensor_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);