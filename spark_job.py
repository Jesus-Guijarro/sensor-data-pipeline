from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("GenerateReport") \
    .getOrCreate()

# Leer datos de SQLite
df = spark.read.format("jdbc").options(
    url="jdbc:sqlite:/home/jfgs/Projects/sensor-data-pipeline/sensor_data.db",
    dbtable="sensor_averages",
    driver="org.sqlite.JDBC"
).load()

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
plt.savefig('/path/to/save/report.png')

# Generar un PDF con reportlab
c = canvas.Canvas("/path/to/save/report.pdf", pagesize=letter)
c.drawImage('/path/to/save/report.png', 50, 500, width=500, height=300)
c.showPage()
c.save()

# Detener la sesión de Spark
spark.stop()
