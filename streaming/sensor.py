import random
import time
import datetime
import numpy as np

class Sensor:
    def __init__(self, sensor_id, diff_temperature=0):
        """
        Initialize a sensor instance.

        :param sensor_id: Unique identifier for the sensor
        :param diff_temperature: Temperature offset relative to the base model (in °C)
        """
        self.sensor_id = sensor_id
        self.diff_temperature = diff_temperature
        self.error_probability = random.uniform(0.001, 0.01)  # Probability of an anomaly

    def generate_normal_data(self):
        """
        Generate normal temperature and humidity values based on season and time of day.

        :return: (temperature: int, humidity: int)
        """
        now = datetime.datetime.now()
        month = now.month
        hour = now.hour

        # Define seasonal ranges for temperature and humidity
        if 3 <= month <= 5:  # Spring
            temp_min, temp_max = 15, 25
            hum_min,  hum_max = 40, 65
        elif 6 <= month <= 8:  # Summer
            temp_min, temp_max = 25, 38
            hum_min,  hum_max = 40, 80
        elif 9 <= month <= 11:  # Autumn
            temp_min, temp_max = 18, 28
            hum_min,  hum_max = 50, 70
        else:  # Winter
            temp_min, temp_max = 5, 15
            hum_min,  hum_max = 50, 70

        # Calculate base and variation for temperature
        base_temp = (temp_min + temp_max) / 2
        var_temp  = (temp_max - temp_min) / 2
        # Sinusoidal variation: min at 5:00, max at 15:00
        omega = 2 * np.pi / 24
        mean_temp = base_temp + var_temp * np.sin((hour - 5) / 10 * np.pi - np.pi/2)
        noise_temp = random.uniform(-1, 1)
        temperature = int(round(mean_temp + noise_temp + self.diff_temperature))

        # Calculate base and variation for humidity
        base_hum = (hum_min + hum_max) / 2
        var_hum  = (hum_max - hum_min) / 2
        # Cosine-based variation: max at 5:00, min at 15:00
        mean_hum = base_hum + var_hum * np.cos((hour - 5) / 10 * np.pi - np.pi/2)
        noise_hum = random.uniform(-3, 3)
        humidity = int(round(mean_hum + noise_hum))
        humidity = max(0, min(100, humidity))  # Clamp between 0 and 100

        return temperature, humidity

    def generate_sensor_data(self):
        """
        Generate sensor data, possibly including anomalies (measurement error or disconnect).

        :return: dict with keys 'sensor_id', 'timestamp', 'temperature', 'humidity', and optional 'anomaly'
        """
        rnd = random.random()
        anomaly = None

        if rnd < self.error_probability:
            anomaly = random.choice(['Measurement_error', 'Disconnect'])
            if anomaly == 'Measurement_error':
                # Apply a large deviation to temperature, keep humidity normal
                normal_temp, normal_hum = self.generate_normal_data()
                offset = random.choice([random.uniform(5, 10), -random.uniform(5, 10)])
                temperature = int(round(normal_temp + offset))
                humidity = normal_hum
            else:  # Disconnect anomaly
                temperature = None
                humidity = None
        else:
            temperature, humidity = self.generate_normal_data()

        data = {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()),
            'temperature': temperature,
            'humidity': humidity
        }

        if anomaly:
            data['anomaly'] = anomaly

        return data
