from pyspark.sql import SparkSession

# Iniciar Spark
spark = SparkSession.builder \
    .appName("AnalisisVentasOnlineRetail") \
    .master("local[*]") \
    .getOrCreate()

# Ubicacion de nuestros archivos CSV
ruta = "/home/vboxuser/Tarea3_Spark/datos/*.csv"

# Cargar los archivos de ventas
ventas = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta)

# Mostrar la estructura
print("ESTRUCTURA DE LOS DATOS")
ventas.printSchema()

# Mostrar los primeros cinco registros
print("PRIMEROS CINCO REGISTROS")
ventas.show(5, truncate=False)

# Contar los registros
print("TOTAL DE REGISTROS")
print(ventas.count())

# Finalizar Spark
spark.stop()

