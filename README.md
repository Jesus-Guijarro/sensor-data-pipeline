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

### Iniciar ZooKeeper

Desde un terminal ejecuta el siguiente comando:

```sh
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties

```
### Iniciar Kafka

```sh
kafka-server-start.sh /opt/kafka/config/server.properties
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



```

```


