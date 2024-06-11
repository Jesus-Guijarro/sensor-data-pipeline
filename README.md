# SENSOR DATA PIPELINE

## 1. Installation and Configuration
### Create the Virtual Environment
```sh
python -m venv sensorenv
```

### Activate the virtual environment
```sh
source sensorenv/bin/activate
```
### Upgrade pip (optional but recommended)
```sh
pip install --upgrade pip
```
### Install the libraries from `requirements.txt`
```sh
pip install -r requirements.txt
```

## Kafka 

### Download
```sh
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
```

### Extract and mov to /opt
```sh
tar -xzf kafka_2.13-3.7.0.tgz && mv kafka_2.13-3.7.0 kafka && sudo mv kafka /opt && rm kafka_2.13-3.7.0.tgz
```

### Environment Variables

```sh
nano ~/.bashrc
```

Add at the end:

```sh
#Kafka
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$KAFKA_HOME/bin
```
Then run:
```sh
source ~/.bashrc
```

### Start ZooKeeper (1st Terminal)

Desde un terminal ejecuta el siguiente comando:

```sh
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties

```
### Start Kafka (2nd Terminal)

```sh
kafka-server-start.sh /opt/kafka/config/server.properties
```

### Create a Topic in Kafka (`sensor-data`) (3rd Terminal)
```sh
kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Run `sensor.py`, which is our `producer`.

###  Verify the Data
```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-data --from-beginning
```

### Delete the Topic
```sh
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sensor-data

```

## Spark

### Download
```sh
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
```

### Extract and move spark to /opt
```sh
tar -xzf spark-3.5.1-bin-hadoop3.tgz && mv spark-3.5.1-bin-hadoop3 spark && sudo mv spark /opt && rm spark-3.5.1-bin-hadoop3.tgz
```

### SPARK_HOME Variable
```sh
nano ~/.bashrc
```
Add at the end:
```sh
#Spark
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
Then run:
```sh
source ~/.bashrc
```
### Test Spark
```sh
spark-shell
```


## Spark -> SQL
```sh
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar
```

Run:
```sh
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  sensor_data_processor.py
```
(Execute once Kafka and sensor.py are running)


## PostgreSQL
Access PostgreSQL:
```sh
psql
```
Create the database:
```sql
CREATE DATABASE sensor_data;
```
Connect to the database:
```
\c sensor_data
```
Create the table:
```sql
CREATE TABLE sensor_averages (
    sensor_id VARCHAR(255),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature REAL,
    avg_humidity REAL
);
```
### Create a `config.ini` File with Database Connection Data

For example:
```ini
[database]
dbname = sensor_data
user = jesus
password = contraseña
host = localhost
port = 5432
```

## Airflow and Spark Job for Batch Processing

Initialize the Airflow database:
```sh
airflow db init
```

Create an admin user:
```sh
airflow users create \
    --username jfgs \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email jfgs@example.com
```
Start the Airflow web server and scheduler:
```sh
airflow webserver --port 8080
```

```sh
airflow scheduler
```

## 2. Run project

### Kafka
Each command in a separate terminal:
```sh
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
```
```sh
kafka-server-start.sh /opt/kafka/config/server.properties
```

Create the `sensor-data` topic:

```sh
kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Sensor
Run `sensor.py` from the terminal with the virtual environment `sensorenv` activated:
```sh
python sensor.py
```

Verify that Kafka is receiving data from the sensors:
```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-data --from-beginning
```
Use Spark Streaming to analyze data in `sensor-data` and send the result to the PostgreSQL database `sensor_data`, table `sensor_averages`:

```sh
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  sensor_data_processor.py
```

### Airflow y Spark
In two different terminals:
```sh
airflow webserver --port 8080
```
```sh
airflow scheduler
```

This is the command that the Airflow DAG should execute:
```sh
spark-submit --jars postgresql-42.7.3.jar spark_job.py
```

o delete the data stored in the `sensor-data` topic:
```sh
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sensor-data
```