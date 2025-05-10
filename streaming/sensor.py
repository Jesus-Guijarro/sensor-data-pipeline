import random
import time
import datetime
import numpy as np

class Sensor:
    """
    Class for simulated temperature and humidity sensors.

    Each sensor instance has a unique ID, a temperature offset (diff_temperature),
    and an error probability determining random anomalies like measurement errors
    or disconnects.
    """
    def __init__(self, sensor_id, diff_temperature):
        """
        Initialize a Sensor object.

        :param sensor_id: Unique identifier for the sensor
        :param diff_temperature: Temperature offset relative to baseline (in °C)
        """
        self.sensor_id = sensor_id
        self.diff_temperature = diff_temperature
        # Randomly determine probability of an anomaly between 0.1% and 1%
        self.error_probability = random.uniform(0.001, 0.01)

    def generate_normal_data(self) -> tuple[int, int]:
        """
        Generate realistic normal temperature and humidity readings.

        Determines seasonal ranges based on current month, applies a sinusoidal
        variation over the day (min at 5:00 and max at 15:00 for temperature,
        inverse for humidity), adds random noise, and clamps humidity to [0, 100].

        :return: A tuple (temperature, humidity) where:
            temperature: int, reading in °C
            humidity: int, relative humidity in %
        """
        now = datetime.datetime.now()
        month = now.month
        hour = now.hour

        # Set seasonal min and max ranges for temperature and humidity
        if 3 <= month <= 5:     # Spring
            temp_min, temp_max = 15, 25
            hum_min, hum_max = 40, 65
        elif 6 <= month <= 8:   # Summer
            temp_min, temp_max = 25, 38
            hum_min, hum_max = 40, 80
        elif 9 <= month <= 11:  # Autumn
            temp_min, temp_max = 18, 28
            hum_min, hum_max = 50, 70
        else:                   # Winter
            temp_min, temp_max = 5, 15
            hum_min, hum_max = 50, 70

        # Compute average and variation for temperature
        base_temp = (temp_min + temp_max) / 2
        var_temp = (temp_max - temp_min) / 2
        # Daily sinusoidal temperature variation: min at 5:00, max at 15:00
        mean_temp = base_temp + var_temp * np.sin((hour - 5) / 10 * np.pi - np.pi / 2)
        # Add small random noise and sensor offset
        noise_temp = random.uniform(-1, 1)
        temperature = int(round(mean_temp + noise_temp + self.diff_temperature))

        # Compute average and variation for humidity
        base_hum = (hum_min + hum_max) / 2
        var_hum = (hum_max - hum_min) / 2
        # Daily cosine humidity variation: peak at 5:00, trough at 15:00
        mean_hum = base_hum + var_hum * np.cos((hour - 5) / 10 * np.pi - np.pi / 2)
        # Add random noise
        noise_hum = random.uniform(-3, 3)
        humidity = int(round(mean_hum + noise_hum))
        # Ensure humidity stays within valid bounds [0, 100]
        humidity = max(0, min(100, humidity))

        return temperature, humidity

    def generate_sensor_data(self) -> dict:
        """
        Generate a data point for the sensor, possibly including an anomaly.

        With a small probability defined by error_probability, the reading may
        include either a large measurement error (anomalous temperature)
        or represent a disconnect.

        :return: dict containing:
            'sensor_id': str, the unique sensor identifier
            'timestamp': int, Unix epoch time of the reading
            'temperature': Optional[int], temperature or None if disconnected
            'humidity': Optional[int], humidity or None if disconnected
            'anomaly': Optional[str], type of anomaly if occurred
        """
        rnd = random.random()
        anomaly = None

        if rnd < self.error_probability:
            # Decide randomly between measurement error or disconnect
            anomaly = random.choice(['Measurement_error', 'Disconnect'])
            if anomaly == 'Measurement_error':
                # Generate normal readings and inject a large temperature offset
                normal_temp, normal_hum = self.generate_normal_data()
                offset = random.choice([
                    random.uniform(5, 10),
                    -random.uniform(5, 10)
                ])
                temperature = int(round(normal_temp + offset))
                humidity = normal_hum
            else:
                # Disconnect: no data readings available
                temperature = None
                humidity = None
        else:
            # Normal operation: generate standard readings
            temperature, humidity = self.generate_normal_data()

        # Build the data payload
        data = {
            'sensor_id': self.sensor_id,
            'timestamp': int(time.time()),
            'temperature': temperature,
            'humidity': humidity
        }

        if anomaly:
            # Include anomaly type in output
            data['anomaly'] = anomaly

        return data
