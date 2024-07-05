# SENSOR DATA PIPELINE

## Introduction
This project focuses on simulating sensor data and processing it through a data pipeline using Kafka, Spark, and Airflow. It serves as a learning exercise to understand the integration and functionalities of these technologies.

## Overview
The goal of this project is to:
- Generate simulated sensor data.
- Use Kafka for real-time data streaming.
- Process the data using Spark for analytics and transformations.
- Orchestrate the pipeline with Airflow for scheduling and monitoring.

## Components
1. Producer (producer.py): simulates sensor data and publishes it to Kafka.
2. Consumer (consumer.py): reads data from Kafka and processes it.
3. Spark Job (sensor_etl_job.py): executes Spark jobs to analyze sensor data.
4. Airflow DAG (sensor_data_dag.py): defines the workflow to automate and monitor the pipeline.

## Installation and Configuration

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

### Kafka 

#### Download Kafka
```sh
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
```

#### Extract and mov to /opt
```sh
tar -xzf kafka_2.13-3.7.0.tgz && mv kafka_2.13-3.7.0 kafka && sudo mv kafka /opt && rm kafka_2.13-3.7.0.tgz
```

#### Environment Variables

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

### Spark

#### Download
```sh
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
```

#### Extract and move spark to /opt
```sh
tar -xzf spark-3.5.1-bin-hadoop3.tgz && mv spark-3.5.1-bin-hadoop3 spark && sudo mv spark /opt && rm spark-3.5.1-bin-hadoop3.tgz
```

#### SPARK_HOME Variable
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
## Test Spark
```sh
spark-shell
```


## Download .jar to connect Spark -> SQL
```sh
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar
```




### PostgreSQL
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
### Create the tables:
```sql
CREATE TABLE sensor_temperatures (
    sensor_id INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    PRIMARY KEY (sensor_id, window_start, window_end)  -- Cambia el índice único a estas columnas
);
```

Table to store hourly aggregated data from the sensors:
```sql
CREATE TABLE sensor_temperatures_hourly (
    sensor_id INT,
    record_date DATE,
    record_hour TIME,
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    PRIMARY KEY (sensor_id, record_date, record_hour)
);

```
#### Create a `config.ini` File with Database Connection Data

For example:
```ini
[database]
dbname = sensor_data
user = jesus
password =  ***
host = localhost
port = 5432
```

## Airflow

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
Start the Airflow web server and scheduler to test if everything is working:
```sh
airflow webserver --port 8080
```

```sh
airflow scheduler
```
Shutdown the web server and scheduler


## Run project

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
Run `producer.py` from the terminal with the virtual environment `sensorenv` activated:
```sh
python producer.py
```

Verify that Kafka is receiving data from the sensors:
```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-data --from-beginning
```
Use Spark Streaming to analyze data in `sensor-data` and send the result to the PostgreSQL database `sensor_data`, table `sensor_averages`:

```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --jars resources/spark-sql-kafka-0-10_2.12-3.0.0.jar consumer.py
```

## Airflow
Copy file `sensor_data_dag.py` in /home/airflow/dags/
```sh
cp sensor_data_dag.py /home/{your_user}/airflow/dags/
```

In two different terminals:
```sh
airflow webserver --port 8080
```
```sh
airflow scheduler
```

Check http://localhost:8080/


If you want to delete the data stored in the `sensor-data` topic:
```sh
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sensor-data
```

## Extra
### Run script to start Kafka and Zookeper servers and to create Kafka topic

Execution permissions
```sh
chmod +x start_kafka.sh
```
### Run script:
```sh
./start_kafka.sh
```