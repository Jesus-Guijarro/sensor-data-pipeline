from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2
import configparser

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
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute"), col("sensor_id")) \
    .agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))

# Leer el archivo de configuración
config = configparser.ConfigParser()
config.read('config.ini')

# Obtener los valores de configuración
db_config = config['database']
DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']


# Función para escribir los resultados en la base de datos
def write_to_db(df, epoch_id):
    # Conectar a la base de datos PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    # Insertar los datos
    for row in df.collect():
        cursor.execute(
            "INSERT INTO sensor_averages (sensor_id, window_start, window_end, avg_temperature, avg_humidity) VALUES (%s, %s, %s, %s, %s)",
            (row['sensor_id'], row['window']['start'], row['window']['end'], row['avg_temperature'], row['avg_humidity'])
        )

    conn.commit()
    conn.close()


# Escribir los resultados en la base de datos cada 15 minutos
query = average_df.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("update") \
    .start()

query.awaitTermination()
