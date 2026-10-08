from kafka import KafkaProducer
import json
import random
import time

# Conectar con Apache Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("PRODUCTOR KAFKA INICIADO")
print("Enviando datos al tema sensor_data...")

try:
    while True:
        # Simular datos de sensores
        datos = {
            "sensor_id": random.randint(1, 10),
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 70), 2),
            "timestamp": int(time.time())
        }

        # Enviar datos a Kafka
        producer.send("sensor_data", value=datos)

        print("Datos enviados:", datos)

        # Esperar un segundo
        time.sleep(1)

except KeyboardInterrupt:
    print("Productor detenido")

finally:
    producer.flush()
    producer.close()
