import glob
import csv
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Buscar el archivo CSV del ultimo lote
ruta = "/home/vboxuser/Tarea3_Spark/resultados_streaming/lote_3/part-*.csv"
archivos = glob.glob(ruta)

if not archivos:
    raise FileNotFoundError("No se encontro el CSV del lote 4")

# Leer los resultados
registros = []

with open(archivos[0], "r", encoding="utf-8") as archivo:
    lector = csv.DictReader(archivo)
    for fila in lector:
        registros.append(fila)

# Obtener la ventana de tiempo mas reciente
ultima_ventana = max(fila["Inicio"] for fila in registros)

# Seleccionar los datos de esa ventana
datos = [
    fila for fila in registros
    if fila["Inicio"] == ultima_ventana
]

# Ordenar por numero de sensor
datos.sort(key=lambda fila: int(fila["sensor_id"]))

sensores = [str(fila["sensor_id"]) for fila in datos]
temperaturas = [float(fila["TemperaturaPromedio"]) for fila in datos]
humedades = [float(fila["HumedadPromedio"]) for fila in datos]

# Crear los graficos
fig, ejes = plt.subplots(2, 1, figsize=(11, 9))

ejes[0].bar(sensores, temperaturas, color="steelblue")
ejes[0].set_title("Temperatura promedio por sensor")
ejes[0].set_xlabel("Sensor")
ejes[0].set_ylabel("Temperatura (°C)")
ejes[0].grid(axis="y", alpha=0.3)

ejes[1].bar(sensores, humedades, color="seagreen")
ejes[1].set_title("Humedad promedio por sensor")
ejes[1].set_xlabel("Sensor")
ejes[1].set_ylabel("Humedad (%)")
ejes[1].grid(axis="y", alpha=0.3)

fig.suptitle(
    "Analisis de sensores - Apache Kafka y Spark Streaming",
    fontsize=14
)

plt.tight_layout()

salida = "/home/vboxuser/Tarea3_Spark/grafico_sensores.png"

plt.savefig(salida, dpi=150, bbox_inches="tight")
plt.close()

print("Grafico creado correctamente:")
print(salida)
print("Ventana analizada:", ultima_ventana)
