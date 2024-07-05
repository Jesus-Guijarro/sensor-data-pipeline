from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import database.database as db

# Create the Spark session
spark = SparkSession.builder \
    .appName("SensorData") \
    .getOrCreate()

# Define the schema for sensor-data
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

# Convert the timestamp to timestamp format and group in 15 minutes windows
sensor_data_df = sensor_data_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Calculate the avg, min and max temperature
df = sensor_data_df \
    .withWatermark("timestamp", "20 minutes") \
    .groupBy(
        window(col("timestamp"), "15 minutes"),
        col("sensor_id")
    ).agg(
        avg(col("temperature")).alias("avg_temperature"),
        min(col("temperature")).alias("min_temperature"),
        max(col("temperature")).alias("max_temperature")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature")
    ).dropDuplicates(["sensor_id", "window_start", "window_end"])

# Function to write the results to the database
def write_to_db(batch_df,batch_id):

    conn, cursor = db.get_connection()
    # Insert the data
    for row in batch_df.collect():
        cursor.execute(
            """
            INSERT INTO sensor_temperatures (sensor_id, window_start, window_end, avg_temperature, min_temperature, max_temperature)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, window_start, window_end) DO UPDATE
            SET avg_temperature = EXCLUDED.avg_temperature,
                min_temperature = EXCLUDED.min_temperature,
                max_temperature = EXCLUDED.max_temperature
            """,
            (row['sensor_id'], row['window_start'], row['window_end'], row['avg_temperature'], row['min_temperature'], row['max_temperature'])
        )

    conn.commit()
    conn.close()

# Write the results to the database every 15 minutes
query = df.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("update") \
    .trigger(processingTime="15 minutes") \
    .start()

query.awaitTermination()