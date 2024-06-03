from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

import configparser
import os

from datetime import datetime, timedelta

# Ruta al archivo JAR del controlador JDBC de PostgreSQL
jdbc_jar_path = "postgresql-42.7.3.jar"

#Ruta archivo
output_dir = os.path.dirname(__file__)

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("GenerateReport") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

# Crear un objeto ConfigParser y leer el archivo config.ini
config = configparser.ConfigParser()
config_path = os.path.join(output_dir, 'config.ini')
config.read(config_path)

# Leer los valores específicos de la sección [postgresql]
db_config = config['database']

DB_NAME = db_config['dbname']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_HOST = db_config['host']
DB_PORT = db_config['port']

# Leer datos de PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
    .option("dbtable", "sensor_averages") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Convertir a Pandas DataFrame para generar el informe
pdf = df.toPandas()

# Generar un gráfico con matplotlib
plt.figure(figsize=(10, 5))
plt.plot(pd.to_datetime(pdf['window_start']), pdf['avg_temperature'], label='Avg Temperature')
plt.plot(pd.to_datetime(pdf['window_start']), pdf['avg_humidity'], label='Avg Humidity')
plt.xlabel('Time')
plt.ylabel('Values')
plt.title('Average Temperature and Humidity Over Time')
plt.legend()
output_path_png = os.path.join(output_dir, 'report.png')
plt.savefig(output_path_png)

now = datetime.now()

pdf_filename = f"report_{now.strftime('%Y-%m-%d')}_{now.strftime('%H-%M-%S')}.pdf"

# Generar un PDF con reportlab
output_path_pdf = os.path.join(output_dir, pdf_filename)
c = canvas.Canvas(output_path_pdf, pagesize=letter)
c.drawImage(output_path_png, 50, 500, width=500, height=300)
c.showPage()
c.save()

# Eliminar el archivo PNG
os.remove(output_path_png)

# Detener la sesión de Spark
spark.stop()
