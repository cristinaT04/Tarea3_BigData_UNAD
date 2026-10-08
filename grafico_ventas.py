import csv
import glob
import os
import matplotlib

# Permite guardar graficos sin pantalla
matplotlib.use("Agg")

import matplotlib.pyplot as plt

# Buscar el CSV generado por Spark
ruta = "/home/vboxuser/Tarea3_Spark/resultados/top_productos/part-*.csv"
archivos = glob.glob(ruta)

if not archivos:
    raise FileNotFoundError("No se encontro el CSV de productos")

productos = []
cantidades = []

with open(archivos[0], "r", encoding="utf-8-sig", newline="") as archivo:
    lector = csv.DictReader(archivo)

    for fila in lector:
        productos.append(fila["Description"])
        cantidades.append(int(fila["UnidadesVendidas"]))

# Crear grafico horizontal
plt.figure(figsize=(12, 7))

plt.barh(productos[::-1], cantidades[::-1], color="steelblue")

plt.xlabel("Unidades vendidas")
plt.ylabel("Productos")
plt.title("Top 10 productos mas vendidos - Online Retail II")

plt.tight_layout()

# Guardar imagen
salida = "/home/vboxuser/Tarea3_Spark/grafico_productos.png"
plt.savefig(salida, dpi=150, bbox_inches="tight")

plt.close()

print("Grafico guardado correctamente en:")
print(salida)
