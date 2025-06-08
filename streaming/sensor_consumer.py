import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_unixtime,
    window,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from psycopg2.extras import execute_batch

import database.database as db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration (override via environment variables)
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/sensor_data")

# Define schema for incoming JSON
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", IntegerType(), True),  # epoch seconds
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

def create_spark_session() -> SparkSession:
    """
    Initialize and return a SparkSession.
    """
    return SparkSession.builder \
        .appName("SensorData") \
        .getOrCreate()


def read_sensor_stream(spark: SparkSession):
    """
    Read sensor data from Kafka and parse JSON payload.
    """
    return (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_SERVERS)
             .option("subscribe", KAFKA_TOPIC)
             .load()
             .selectExpr("CAST(value AS STRING) as json")
             .select(from_json(col("json"), schema).alias("data"))
             .select("data.*")
             .withColumn(
                 "timestamp", to_timestamp(from_unixtime(col("timestamp")))
             )
    )


def compute_aggregates(sensor_df):
    """
    Compute 5-minute windowed averages for temperature and humidity.
    """
    return (
        sensor_df
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("sensor_id")
            )
            .agg(
                avg("temperature").alias("avg_temperature"),
                avg("humidity").alias("avg_humidity")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("sensor_id"),
                col("avg_temperature"),
                col("avg_humidity")
            )
    )


def upsert_partition(partition):
    """
    Upsert a partition of rows into the database using batch execution.
    """
    conn, cursor = db.get_connection()
    try:
        sql = """
            INSERT INTO sensor_readings (
                sensor_id, window_start, window_end, temperature, humidity
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, window_start, window_end) DO UPDATE
              SET temperature = EXCLUDED.temperature,
                  humidity    = EXCLUDED.humidity
        """
        args = [
            (
                row.sensor_id,
                row.window_start,
                row.window_end,
                row.avg_temperature,
                row.avg_humidity
            )
            for row in partition
        ]
        if args:
            execute_batch(cursor, sql, args)
            conn.commit()
            logger.info("Upserted %d records", len(args))
    except Exception as e:
        logger.exception("Error upserting partition: %s", e)
    finally:
        cursor.close()
        conn.close()


def write_to_db(batch_df, batch_id):
    """
    Write each micro-batch to the database using foreachPartition for efficiency.
    """
    logger.info("Processing batch %s", batch_id)
    batch_df.foreachPartition(upsert_partition)


def main():
    """
    Main entry point for the Spark streaming job.
    """
    spark = create_spark_session()
    sensor_stream = read_sensor_stream(spark)
    agg_df = compute_aggregates(sensor_stream)

    query = (
        agg_df.writeStream
             .foreachBatch(write_to_db)
             .outputMode("update")
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime="5 minutes")
             .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
