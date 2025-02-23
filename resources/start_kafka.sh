#!/bin/bash

# Open a new terminal and run Zookeeper
gnome-terminal -- bash -c "zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties; exec bash"

# Wait a few seconds to ensure that Zookeeper is fully started
sleep 5

# Open a new terminal and run Kafka server
gnome-terminal -- bash -c "kafka-server-start.sh /opt/kafka/config/server.properties; exec bash"

# Wait a few seconds to ensure that the Kafka server is fully started.
sleep 5

# Open a new terminal and create the topics sensor-data and log-data
gnome-terminal -- bash -c "kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1; exec bash"

gnome-terminal -- bash -c "kafka-topics.sh --create --topic log-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1; exec bash"