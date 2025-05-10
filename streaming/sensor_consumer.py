from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import database.database as db

spark = SparkSession.builder \
    .appName("SensorData") \
    .getOrCreate()

# Define the schema for sensor-data including humidity
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", IntegerType(), True),  # epoch seconds
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Read data from Kafka topic 'sensor-data'
sensor_data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# Parse JSON payload and apply schema
sensor_data_df = sensor_data_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Convert epoch seconds to TimestampType
sensor_data_df = sensor_data_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Calculate 5-minute windowed averages for temperature and humidity
agg_df = sensor_data_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        avg(col("temperature")).alias("temperature"),
        avg(col("humidity")).alias("humidity")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("temperature"),
        col("humidity")
    ) \
    .dropDuplicates(["sensor_id", "window_start", "window_end"])


def write_to_db(batch_df, batch_id):
    """
    Function to write each micro-batch of aggregated data into the 
    'sensor_readings' table.

    Uses UPSERT logic: inserts new records or updates existing ones based on
    primary key (sensor_id, window_start, window_end).

    :param batch_df: Spark DataFrame containing one micro-batch of aggregated rows
    """
    conn, cursor = db.get_connection()
    for row in batch_df.collect():
        cursor.execute(
            """
            INSERT INTO sensor_readings (
                sensor_id, window_start, window_end, temperature, humidity
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, window_start, window_end) DO UPDATE
            SET temperature = EXCLUDED.temperature,
                humidity    = EXCLUDED.humidity
            """,
            (
                row['sensor_id'],
                row['window_start'],
                row['window_end'],
                row['temperature'],
                row['humidity']
            )
        )
    conn.commit()
    conn.close()

# Configure the streaming query to use write_to_db every 5 minutes
query = (
    agg_df.writeStream
          .foreachBatch(write_to_db)            # Use custom function for each micro-batch
          .outputMode("update")                 # Only updated aggregates are written
          .trigger(processingTime="5 minutes")  # Trigger processing interval
          .start()                              # Start the streaming query
)

# Await termination of the streaming query (runs indefinitely)
query.awaitTermination()
