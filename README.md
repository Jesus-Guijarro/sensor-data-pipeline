# sensor-data-pipeline

## 1. Installation and configuration
### Create the virtual environment
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
### Install the libraries from requirements.txt
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

### Variables

```sh
nano ~/.bashrc
```

Write at the end:

```sh
#Kafka
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$KAFKA_HOME/bin
```

```sh
source ~/.bashrc
```

### Iniciar ZooKeeper (1º terminal)

Desde un terminal ejecuta el siguiente comando:

```sh
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties

```
### Iniciar Kafka (2º terminal)

```sh
kafka-server-start.sh /opt/kafka/config/server.properties
```

### Crear un topic en Kafka (sensor-data) (3º terminal)
```sh
kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Ejecutar `sensor.py`, que es nuestro `producer`.

###  Verificar los datos
```sh
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-data --from-beginning
```

### Borrar el topic
```sh
kafka-topics.sh --zookeeper localhost:9092 --delete --topic sensor-data
```

<br>

## Spark

### Download
```sh
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
```

### Extract and move spark to /opt
```sh
tar -xzf spark-3.5.1-bin-hadoop3.tgz && mv spark-3.5.1-bin-hadoop3 spark && sudo mv spark /opt && rm spark-3.5.1-bin-hadoop3.tgz
```

### Variable SPARK_HOME
```sh
nano ~/.bashrc
```

```sh
#Spark
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

```sh
source ~/.bashrc
```
### Test Spark
```sh
spark-shell
```


<br>


## Spark -> SQLite
```sh
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar
```
```sh
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  sensor_data_processor.py
```
Ejecutar una vez kafka y sensor.py esté en ejecución

