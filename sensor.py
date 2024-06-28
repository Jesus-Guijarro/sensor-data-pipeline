import random
import time
import logging

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.anomaly_probability = random.uniform(0.01, 0.05)

    def generate_sensor_data(self):

        random_value = random.random()

        # Check if an anomaly should be generated
        if random_value < self.anomaly_probability:
            # Generate anomalous data
            anomaly_type = random.choice(['extreme', 'disconnect'])
            if anomaly_type == 'extreme':
                temperature = round(random.choice([random.uniform(-20, -10), random.uniform(50, 60)]), 2)
                logging.warning(f'Sensor ID {self.sensor_id}: anomalous extreme temperature detected: {temperature}°C')
            # Disconnection
            elif anomaly_type == 'disconnect':
                temperature = 0
                logging.error(f'Sensor ID {self.sensor_id}: disconnected')
        else:
            temperature = round(random.uniform(15, 30), 2)  # Normal temperature between 15 and 30 degrees

        return {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()), # Current time as timestamp
            'temperature': temperature
        }