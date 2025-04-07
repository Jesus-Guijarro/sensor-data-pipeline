import random
import time
import datetime
import numpy as np

class Sensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        self.error_probability = random.uniform(0.001, 0.01)  # Some sensors have a higher probability of error
    
    def generate_normal_data(self):
        """
        Function to generate a normal temperature value (integer) with daily variation using a sinusoidal model.
        """
        now = datetime.datetime.now()
        month = now.month

        # Base temperature and daily variation for different seasons
        if 3 <= month <= 5:     # Spring
            base_temperature = 18  
            daily_variation = 4
        elif 6 <= month <= 8:   # Summer
            base_temperature = 28  
            daily_variation = 5
        elif 9 <= month <= 11:  # Autumn
            base_temperature = 24  
            daily_variation = 4
        else:                   # Winter
            base_temperature = 15 
            daily_variation = 3

        hour = now.hour
        # Using a sinusoidal function for daily temperature variation:
        # Assuming minimum at 5:00 (base_temperature - daily_variation)
        # and maximum at 15:00 (base_temperature + daily_variation)
        mean_temp = base_temperature + daily_variation * np.sin((hour - 5) / 10 * np.pi - np.pi/2)

        # Generate random temperature from a normal distribution and convert to integer
        noise = random.uniform(-1, 1)  # for a variation of ±1 degree
        temperature = int(round(mean_temp + noise))
        
        return temperature
    
    def generate_sensor_data(self):
        """
        Function for generating sensor data.
        """
        random_value = random.random()
        anomaly_type = False

        # Check if an anomaly should be generated
        if random_value < self.error_probability:
            anomaly_type = random.choice(['Measurement_error', 'Disconnect'])
            if anomaly_type == 'Measurement_error':
                # Generate a measurement error with a deviation of at least 5 degrees from the normal temperature
                normal_temp = self.generate_normal_data()
                offset = random.choice([random.uniform(5, 10), -random.uniform(5, 10)])
                temperature = int(round(normal_temp + offset))
            elif anomaly_type == 'Disconnect':
                temperature = None
        else:
            temperature = self.generate_normal_data()

        data = {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()),
            'temperature': temperature
        }

        if anomaly_type:
            data['anomaly'] = anomaly_type
            return data

        return data
