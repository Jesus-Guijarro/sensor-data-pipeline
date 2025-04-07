from confluent_kafka import Consumer, KafkaError
import json

# Consumer configuration
log_topic = 'log-data'
bootstrap_servers = 'localhost:9092'
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'log-consumer-group',
    'auto.offset.reset': 'earliest'
}

def consume_logs():
    """
    Consumes log messages from the Kafka topic log-data
    """
    consumer = Consumer(consumer_conf)
    consumer.subscribe([log_topic])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg and msg.value():
                log_entry = json.loads(msg.value().decode('utf-8'))
                print(f"[{log_entry['level']}] {log_entry['message']}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
