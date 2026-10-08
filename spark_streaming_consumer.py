from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime,
    to_timestamp, window, avg
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, LongType
)

# Iniciar Spark
spark = SparkSession.builder \
    .appName("SensoresKafkaStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Estructura de los mensajes enviados por Kafka
esquema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

# Conectar Spark con el tema sensor_data
datos_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir mensajes JSON en columnas
datos = datos_kafka.select(
    from_json(
        col("value").cast("string"),
        esquema
    ).alias("datos")
).select("datos.*")

# Convertir la fecha de los sensores
datos = datos.withColumn(
    "fecha",
    to_timestamp(from_unixtime(col("timestamp")))
)

# Calcular promedios por sensor y por minuto
promedios = datos.groupBy(
    window(col("fecha"), "1 minute"),
    col("sensor_id")
).agg(
    avg("temperature").alias("TemperaturaPromedio"),
    avg("humidity").alias("HumedadPromedio")
)

# Guardar los promedios en CSV y mostrarlos en pantalla
import os

carpeta = "/home/vboxuser/Tarea3_Spark/resultados_streaming"
os.makedirs(carpeta, exist_ok=True)

def guardar_resultados(df, batch_id):
    if df.count() > 0:
        # Preparar columnas para guardar en CSV
        salida = df.select(
            col("window.start").cast("string").alias("Inicio"),
            col("window.end").cast("string").alias("Fin"),
            col("sensor_id"),
            col("TemperaturaPromedio"),
            col("HumedadPromedio")
        )

        print("RESULTADOS DEL LOTE:", batch_id)
        salida.show(20, truncate=False)

        # Guardar cada lote en su propia carpeta
        salida.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{carpeta}/lote_{batch_id}")

# Procesar los datos en tiempo real
consulta = promedios.writeStream \
    .outputMode("complete") \
    .foreachBatch(guardar_resultados) \
    .trigger(processingTime="10 seconds") \
    .start()

consulta.awaitTermination()
