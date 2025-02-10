import random
import time
import datetime
import logging
import numpy as np

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.anomaly_type = random.choice(['extreme_value', 'disconnect'])
        self.error_probability = random.uniform(0.001, 0.01) # Some sensors have a higher probability of disconnect or generate a extreme value
    
    def generate_normal_data(self):
        """
        Function to generate a normal temperature value
        """
        now = datetime.datetime.now()

        month = now.month

        # Average temperatures and standard deviation for different times and seasons
        if 3 <= month <= 5:     # Spring
            base_temperature  = 15  
        elif 6 <= month <= 8:   # Summer
            base_temperature  = 25  
        elif 9 <= month <= 11:  # Autumn
            base_temperature  = 20  
        else:                   # Winter
            base_temperature  = 12 

        hour = now.hour
        daily_temperature_variation  = 6 
        # Calculate the mean and standard deviation as a function of time of day.
        if 6 <= hour < 18: # Daytime
            mean_temp = base_temperature  + (daily_temperature_variation  * (hour - 6) / 12)
        else:              # Nighttime
            mean_temp = base_temperature  - (daily_temperature_variation  * ((hour - 18) % 24) / 12)

        std_dev_temp = 2  # Fixed standard deviation for simplification

        # Generate random temperature
        temperature = np.random.normal(mean_temp, std_dev_temp)
        
        return temperature
    
    def generate_sensor_data(self):
        """
        Function for generating sensor data
        """
        random_value = random.random()

        # Check if an anomaly should be generated
        if random_value < self.error_probability:
            # Generate anomalous data
            anomaly_type = random.choice(['extreme_value', 'disconnect'])
            if self.anomaly_type == 'extreme_value':
                temperature = round(random.choice([random.uniform(-20, -10), random.uniform(50, 60)]), 2)
                logging.warning(f'Sensor ID {self.sensor_id}: anomalous extreme temperature detected: {temperature}°C')
            # Sensor "Disconnection"
            elif self.anomaly_type == 'disconnect':
                temperature = None
                logging.error(f'Sensor ID {self.sensor_id}: disconnected')
        else:
            temperature = self.generate_normal_data()

        return {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()),
            'temperature': temperature
        }   