#!/bin/bash

# Abrir nueva terminal y ejecutar Zookeeper
gnome-terminal -- bash -c "zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties; exec bash"

# Esperar unos segundos para asegurar que Zookeeper esté completamente iniciado
sleep 5

# Abrir nueva terminal y ejecutar Kafka server
gnome-terminal -- bash -c "kafka-server-start.sh /opt/kafka/config/server.properties; exec bash"

# Esperar unos segundos para asegurar que Kafka server esté completamente iniciado
sleep 5

# Abrir nueva terminal y crear el topic
gnome-terminal -- bash -c "kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1; exec bash"