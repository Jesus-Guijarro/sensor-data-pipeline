import random
import time
from kafka import KafkaProducer
import json

# Configuración de Kafka
bootstrap_servers = 'localhost:9092'
topic = 'sensor-data'

# Configuración del producer de Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Función para generar datos de sensores
def generate_sensor_data(sensor_id):
    # Generar datos normales
    temperature = round(random.uniform(20, 25), 2)  # Temperatura normal entre 20 y 25 grados
    humidity = round(random.uniform(30, 50), 2)     # Humedad normal entre 30 y 50%

    return {
        'sensor_id': sensor_id,
        'timestamp': int(time.time()),
        'temperature': temperature,
        'humidity': humidity
    }

# Enviar datos continuamente
sensor_count = 50  # Número de sensores
while True:
    for sensor_id in range(sensor_count):
        data = generate_sensor_data(sensor_id)
        producer.send(topic, value=data)
    time.sleep(1)  # Enviar datos cada segundo
