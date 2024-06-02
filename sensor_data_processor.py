from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sqlite3

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("SensorDataProcessing") \
    .getOrCreate()

# Definir el esquema para los datos del sensor
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Leer datos de Kafka
sensor_data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# Convertir los datos del valor de Kafka a formato JSON y aplicar el esquema
sensor_data_df = sensor_data_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Convertir el timestamp a formato de tiempo y agrupar en ventanas de 15 minutos
sensor_data_df = sensor_data_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Calcular la media de temperatura y humedad en ventanas de 15 minutos
average_df = sensor_data_df \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(window(col("timestamp"), "15 minutes")) \
    .agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))

# Función para escribir los resultados en la base de datos
def write_to_db(df, epoch_id):
    # Conectar a la base de datos (SQLite en este caso)
    conn = sqlite3.connect('sensor_data.db')
    cursor = conn.cursor()

    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensor_averages (
            window_start TEXT,
            window_end TEXT,
            avg_temperature REAL,
            avg_humidity REAL
        )
    ''')

    # Insertar los datos
    for row in df.collect():
        cursor.execute("INSERT INTO sensor_averages (window_start, window_end, avg_temperature, avg_humidity) VALUES (?, ?, ?, ?)",
                       (row['window']['start'], row['window']['end'], row['avg_temperature'], row['avg_humidity']))

    conn.commit()
    conn.close()

# Escribir los resultados en la base de datos cada 15 minutos
query = average_df.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("update") \
    .start()

query.awaitTermination()
