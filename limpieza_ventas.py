from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Iniciar Spark
spark = SparkSession.builder \
    .appName("LimpiezaVentas") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Cargar los archivos CSV
ruta = "/home/vboxuser/Tarea3_Spark/datos/*.csv"

ventas = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta)

# Contar registros originales
print("TOTAL DE REGISTROS ORIGINALES")
print(ventas.count())

# Eliminar registros sin datos esenciales
ventas_limpias = ventas.dropna(
    subset=["Invoice", "StockCode", "Quantity", "Price"]
)

# Excluir facturas canceladas
ventas_limpias = ventas_limpias.filter(
    ~col("Invoice").startswith("C")
)

# Excluir cantidades y precios no positivos
ventas_limpias = ventas_limpias.filter(
    (col("Quantity") > 0) &
    (col("Price") > 0)
)

# Calcular el ingreso de cada registro
ventas_limpias = ventas_limpias.withColumn(
    "TotalVenta",
    col("Quantity") * col("Price")
)

# Mostrar resultados
print("TOTAL DE REGISTROS DESPUES DE LA LIMPIEZA")
print(ventas_limpias.count())

print("PRIMERAS CINCO VENTAS LIMPIAS")
ventas_limpias.show(5, truncate=False)
from pyspark.sql.functions import sum as spark_sum, desc

# Analizar los productos mas vendidos
print("TOP 10 PRODUCTOS MAS VENDIDOS")

productos = ventas_limpias.groupBy(
    "StockCode", "Description"
).agg(
    spark_sum("Quantity").alias("UnidadesVendidas")
).orderBy(
    desc("UnidadesVendidas")
)

productos.show(10, truncate=False)

# Calcular ingresos totales
print("INGRESOS TOTALES")

ventas_limpias.agg(
    spark_sum("TotalVenta").alias("IngresosTotales")
).show()
# Guardar los 10 productos mas vendidos
print("GUARDANDO RESULTADOS PARA GRAFICOS")

productos.limit(10) \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/home/vboxuser/Tarea3_Spark/resultados/top_productos")
from pyspark.sql.functions import (
    to_timestamp, date_format,
    sum as spark_sum, desc
)

# Convertir las fechas de las transacciones
ventas_fechas = ventas_limpias.withColumn(
    "Fecha",
    to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
)

# Agrupar las ventas por mes
ventas_mensuales = ventas_fechas.filter(
    col("Fecha").isNotNull()
).withColumn(
    "Mes", date_format(col("Fecha"), "yyyy-MM")
).groupBy("Mes").agg(
    spark_sum("TotalVenta").alias("IngresosMensuales")
).orderBy("Mes")

print("INGRESOS POR MES")
ventas_mensuales.show(30, truncate=False)

# Guardar resultados para crear el grafico
ventas_mensuales.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/home/vboxuser/Tarea3_Spark/resultados/ventas_mensuales")



spark.stop()

