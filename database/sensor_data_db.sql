
-- CREATE sensor_data DATABASE
CREATE DATABASE sensor_data;

-- CREATE sensors TABLE
CREATE TABLE sensors (
  sensor_id INTEGER PRIMARY KEY,
  city VARCHAR(100) NOT NULL,
  station VARCHAR(100) NOT NULL,
  diff_temperature INTEGER NOT NULL
);

-- INSERT DATA IN sensors
-- Alicante is the city used as the baseline for "diff_temperature"
INSERT INTO sensors (sensor_id, city, station, diff_temperature) VALUES
  (1,'Valencia','VALENCIA, UPV',-1),
  (2,'Murcia','MURCIA',1),
  (3,'Alicante','ALACANT/ALICANTE',0),
  (4,'Elche','ALICANTE-ELCHE AEROPUERTO',0),
  (5,'Cartagena','CARTAGENA',1),
  (6,'Almería','ALMERÍA AEROPUERTO',1),
  (7,'Lorca','LORCA',2),
  (8,'El Ejido','EL EJIDO',1),
  (9,'Orihuela','ORIHUELA',1),
  (10,'Molina de Segura','MOLINA DE SEGURA',-1),
  (11,'Benidorm','BENIDORM',0),
  (12,'Sagunto','SAGUNT/SAGUNTO',-1),
  (13,'Alcoy','ALCOI/ALCOY',-2),
  (14,'Torre-Pacheco','TORRE-PACHECO',1),
  (15,'Águilas','ÁGUILAS',1),
  (16,'Ontinyent','ONTINYENT',-2),
  (17,'Yecla','YECLA',-2),
  (18,'Cieza','CIEZA',2),
  (19,'Villena','VILLENA',-2),
  (20,'Totana', 'TOTANA',-1);

-- CREATE sensors_readings TABLE
CREATE TABLE sensor_readings(
  sensor_id INT NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  temperature INTEGER,
  humidity INTEGER,
  PRIMARY KEY (sensor_id, window_start, window_end),
  FOREIGN KEY (sensor_id)
  REFERENCES sensors(sensor_id)
  ON UPDATE CASCADE
  ON DELETE RESTRICT
);