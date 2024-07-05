import random
import time
import datetime
import logging
import numpy as np

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.anomaly_probability = random.uniform(0.01, 0.05)
    
    def generate_normal_data(self):
        """
        Function to generate a normal temperature value
        """
        now = datetime.datetime.now()
        hour = now.hour
        month = now.month

        # Average temperatures and standard deviation for different times and seasons
        if 3 <= month <= 5:  # Spring
            base_temp = 15  
            temp_variation_day = 6  
        elif 6 <= month <= 8:  # Summer
            base_temp = 25  
            temp_variation_day = 6  
        elif 9 <= month <= 11:  # Autumn
            base_temp = 20  
            temp_variation_day = 6  
        else:  # Winter
            base_temp = 12  
            temp_variation_day = 6 

        # Calculate the mean and standard deviation as a function of time of day.
        if 6 <= hour < 18: 
            mean_temp = base_temp + (temp_variation_day * (hour - 6) / 12)
        else: 
            if hour < 6:
                mean_temp = base_temp - (temp_variation_day * (6 - hour) / 6)
            else:
                mean_temp = base_temp - (temp_variation_day * (hour - 15) / 6)

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
        if random_value < self.anomaly_probability:
            # Generate anomalous data
            anomaly_type = random.choice(['extreme', 'disconnect'])
            if anomaly_type == 'extreme':
                temperature = round(random.choice([random.uniform(-20, -10), random.uniform(50, 60)]), 2)
                logging.warning(f'Sensor ID {self.sensor_id}: anomalous extreme temperature detected: {temperature}°C')
            # Sensor "Disconnection"
            elif anomaly_type == 'disconnect':
                temperature = 0
                logging.error(f'Sensor ID {self.sensor_id}: disconnected')
        else:
            temperature = self.generate_normal_data()

        return {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()), # Current time as timestamp
            'temperature': temperature
        }   