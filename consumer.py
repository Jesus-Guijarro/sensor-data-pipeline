from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import psycopg2
import configparser

# Create the Spark session
spark = SparkSession.builder \
    .appName("SensorDataProcessing") \
    .getOrCreate()

# Define the schema for sensor data
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", IntegerType(), True),
    StructField("temperature", DoubleType(), True)
])

# Read data from Kafka
sensor_data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# Convert Kafka data value to JSON format and apply the schema
sensor_data_df = sensor_data_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Convert the timestamp to timestamp format and group in 1 hour windows
sensor_data_df = sensor_data_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Calculate the avg, min and max temperature in 1 hour windows
df = sensor_data_df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window(col("timestamp"), "1 hour").alias("hourly_window"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature")
    )\
    .select(
        col("sensor_id"),
        col("hourly_window").getField("start").alias("timestamp"),
        col("avg_temperature").alias("temperature.average"),
        col("min_temperature").alias("temperature.min"),
        col("max_temperature").alias("temperature.max")
    )

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration values
db_config = config['database']
DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']


# Function to write the results to the database
def write_to_db(df, epoch_id):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    # Insert the data
    for row in df.collect():
        cursor.execute(
            "INSERT INTO sensor_averages (sensor_id, window_start, window_end, avg_temperature) VALUES (%s, %s, %s, %s, %s)",
            (row['sensor_id'], row['window']['start'], row['window']['end'], row['avg_temperature'])
        )

    conn.commit()
    conn.close()


# Write the results to the database every 15 minutes
query = df.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("update") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
