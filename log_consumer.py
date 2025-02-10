from kafka import KafkaConsumer
import json

# Consumer configuration
log_topic = 'log-data'
bootstrap_servers = ['localhost:9092']

def consume_logs():
    consumer = KafkaConsumer(
        log_topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print("Consuming topic logs...")
    for message in consumer:
        log_entry = message.value
        print(f"[{log_entry['level']}] {log_entry['message']}")

if __name__ == "__main__":
    consume_logs()