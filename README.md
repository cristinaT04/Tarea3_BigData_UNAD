# Tarea 3 – Procesamiento de Datos con Apache Spark

**Universidad Nacional Abierta y a Distancia (UNAD)**  
**Curso:** Big Data  
**Actividad:** Tarea 3 – Procesamiento de Datos con Apache Spark

## Descripción del proyecto

Este repositorio contiene los programas desarrollados para la Tarea 3 del curso Big Data. El objetivo es aplicar herramientas de procesamiento de datos por lotes y en tiempo real utilizando Apache Spark, Apache Kafka y Python.

## Procesamiento por lotes

Se utilizó el conjunto de datos Online Retail II, que contiene información de transacciones comerciales.

Mediante PySpark se realizaron actividades de carga, exploración, limpieza y transformación de datos. También se analizaron los productos más vendidos y los ingresos mensuales.

Los resultados se representaron mediante gráficos elaborados con Python y Matplotlib.

## Procesamiento en tiempo real

Se configuró Apache Kafka y se creó el tema `sensor_data` para recibir datos simulados de sensores.

Se desarrolló un productor en Python que envía información de temperatura y humedad. Posteriormente, Apache Spark Structured Streaming recibe los mensajes y calcula los promedios por sensor en ventanas de un minuto.

Los resultados se guardaron en archivos CSV y se visualizaron mediante gráficos de barras.

## Archivos del proyecto

- `analisis_ventas.py`: carga y exploración de datos.
- `limpieza_ventas.py`: limpieza y análisis de ventas.
- `grafico_ventas.py`: visualización de productos más vendidos.
- `grafico_mensual.py`: visualización de ingresos mensuales.
- `kafka_producer.py`: envío de datos simulados a Kafka.
- `spark_streaming_consumer.py`: procesamiento de datos en tiempo real.
- `grafico_sensores.py`: visualización de temperatura y humedad.

## Herramientas utilizadas

- Ubuntu y Oracle VirtualBox.
- Apache Spark y PySpark.
- Apache Kafka y ZooKeeper.
- Python y Matplotlib.
- Microsoft Excel para preparar los archivos CSV.
- GitHub para almacenar el código.

## Conjunto de datos

Se utilizó Online Retail II para el procesamiento por lotes. Los archivos originales no se incluyen en el repositorio debido a su tamaño.

Para el procesamiento en tiempo real se utilizaron datos simulados de diez sensores.

## Resultados

Se logró procesar información comercial por lotes y analizar datos simulados de sensores en tiempo real, utilizando Apache Spark y Apache Kafka.

El proyecto permitió comprender cómo estas herramientas facilitan el procesamiento, análisis y visualización de grandes volúmenes de información.
