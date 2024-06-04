from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import configparser
import os
from datetime import datetime

# Path to the PostgreSQL JDBC driver JAR file
jdbc_jar_path = "postgresql-42.7.3.jar"

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

# Convert to Pandas DataFrame to generate the report
pdf = df.toPandas()

# Generate a plot with matplotlib
plt.figure(figsize=(10, 5))
plt.plot(pd.to_datetime(pdf['window_start']), pdf['avg_temperature'], label='Avg Temperature')
plt.plot(pd.to_datetime(pdf['window_start']), pdf['avg_humidity'], label='Avg Humidity')
plt.xlabel('Time')
plt.ylabel('Values')
plt.title('Average Temperature and Humidity Over Time')
plt.legend()
output_path_png = os.path.join(output_dir, 'report.png')
plt.savefig(output_path_png)

# Get current time for timestamping the PDF
now = datetime.now()

# Create the PDF filename with timestamp
pdf_filename = f"report_{now.strftime('%Y-%m-%d')}_{now.strftime('%H-%M-%S')}.pdf"

# Generate a PDF with reportlab
output_path_pdf = os.path.join(output_dir, pdf_filename)
c = canvas.Canvas(output_path_pdf, pagesize=letter)
c.drawImage(output_path_png, 50, 500, width=500, height=300)
c.showPage()
c.save()

# Remove the PNG file
os.remove(output_path_png)

# Stop the Spark session
spark.stop()
