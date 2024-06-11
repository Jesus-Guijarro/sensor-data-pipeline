from pyspark.sql import SparkSession
import configparser
import os

# Path to the PostgreSQL JDBC driver JAR file
jdbc_jar_path = "jars/postgresql-42.7.3.jar"

# File path
output_dir = os.path.dirname(__file__)

# Create the Spark session
spark = SparkSession.builder \
    .appName("GenerateReport") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

# Create a ConfigParser object and read the config.ini file
config = configparser.ConfigParser()
config_path = os.path.join(output_dir, 'config.ini')
config.read(config_path)

# Get the configuration values
db_config = config['database']

DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']

# Read data from PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
    .option("dbtable", "sensor_averages") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()


# Stop the Spark session
spark.stop()
