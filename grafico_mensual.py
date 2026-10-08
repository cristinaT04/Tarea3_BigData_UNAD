import csv
import glob
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Buscar el CSV generado por Spark
ruta = "/home/vboxuser/Tarea3_Spark/resultados/ventas_mensuales/part-*.csv"
archivos = glob.glob(ruta)

if not archivos:
    raise FileNotFoundError("No se encontro el archivo de ventas mensuales")

meses = []
ingresos = []

with open(archivos[0], "r", encoding="utf-8-sig") as archivo:
    lector = csv.DictReader(archivo)

    for fila in lector:
        meses.append(fila["Mes"])
        ingresos.append(float(fila["IngresosMensuales"]))

# Crear el grafico
plt.figure(figsize=(13, 6))

plt.plot(meses, ingresos, marker="o", linewidth=2)

plt.title("Ingresos mensuales - Online Retail II")
plt.xlabel("Mes")
plt.ylabel("Ingresos (GBP)")
plt.xticks(rotation=60)
plt.grid(True, alpha=0.3)

plt.tight_layout()

# Guardar la imagen
salida = "/home/vboxuser/Tarea3_Spark/grafico_mensual.png"

plt.savefig(salida, dpi=150, bbox_inches="tight")
plt.close()

print("Grafico mensual guardado correctamente:")
print(salida)
