import random
import logging

# Configurar logging
logging.basicConfig(filename='sensor_logs.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Probabilidad de que ocurra una anomalía
anomaly_probability = 0.1

def generate_sensor_data():
    random_value = random.random()
    
    # Check if an anomaly should be generated
    if random_value < anomaly_probability:
        # Generate anomalous data
        anomaly_type = random.choice(['extreme', 'disconnect'])
        if anomaly_type == 'extreme':
            temperature = round(random.choice([random.uniform(-20, -10), random.uniform(50, 60)]), 2)
            logging.warning(f'Anomalous extreme temperature detected: {temperature}°C')
        # Disconnection
        elif anomaly_type == 'disconnect':
            temperature = None  # Null temperature
            logging.error('Sensor disconnected')
    else:
        temperature = round(random.uniform(15, 30), 2)  # Normal temperature between 15 and 30 degrees
    
    return temperature

# Generar datos del sensor en un loop
for _ in range(100):  # Por ejemplo, generar 100 lecturas de sensor
    generate_sensor_data()