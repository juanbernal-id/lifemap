"""
E2.2 - Productor con reloj acelerado.

Publica al topic "reviews" las reseñas con date >= FECHA_CORTE_T, en orden
de fecha, con velocidad configurable (por defecto, el equivalente a un mes
de dataset cada SEGUNDOS_POR_MES segundos de reloj real).

FECHA_CORTE_T es la misma fecha de corte que usa el modelo de riesgo
(spark_jobs/riesgo_cierre.py, E2.6): estas reseñas no se usan para calcular
las variables de ningun negocio hasta T, se reservan para reproducirlas acá
como si llegaran en vivo.

Clave del mensaje: business_id. Ademas de ser el requisito de E2.2, garantiza
que todas las reseñas de un mismo negocio caen en la misma particion del
topic, lo que mantiene su orden relativo para el consumidor.
"""
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "reviews")
FECHA_CORTE_T = datetime.fromisoformat(
    os.environ.get("FECHA_CORTE_T", "2021-01-01T00:00:00+00:00")
)
SEGUNDOS_POR_MES = float(os.environ.get("SEGUNDOS_POR_MES", "10"))
LIMITE_MENSAJES = os.environ.get("LIMITE_MENSAJES")  # para la prueba de humo de Jenkins (E2.9)


def mes_de(fecha):
    return (fecha.year, fecha.month)


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    filtro = {"date": {"$gte": FECHA_CORTE_T}}
    total = db["reviews"].count_documents(filtro)
    cursor = db["reviews"].find(
        filtro,
        {"_id": 0, "review_id": 1, "business_id": 1, "stars": 1, "text": 1, "date": 1},
    ).sort("date", 1)
    if LIMITE_MENSAJES:
        cursor = cursor.limit(int(LIMITE_MENSAJES))
        total = min(total, int(LIMITE_MENSAJES))

    print(
        f"Productor: {total:,} reseñas reservadas desde {FECHA_CORTE_T.date()} "
        f"(fuera del entrenamiento). Velocidad: 1 mes de dataset cada {SEGUNDOS_POR_MES}s."
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        linger_ms=50,
    )

    mes_actual = None
    enviados = 0
    for doc in cursor:
        fecha = doc["date"]
        if fecha.tzinfo is None:
            fecha = fecha.replace(tzinfo=timezone.utc)
        mes = mes_de(fecha)
        if mes_actual is not None and mes != mes_actual and not LIMITE_MENSAJES:
            print(f"  -> avanzando de {mes_actual} a {mes}: durmiendo {SEGUNDOS_POR_MES}s (reloj acelerado)")
            time.sleep(SEGUNDOS_POR_MES)
        mes_actual = mes

        mensaje = {
            "review_id": doc.get("review_id"),
            "business_id": doc["business_id"],
            "stars": doc.get("stars"),
            "text": doc.get("text") or "",
            "date": fecha.isoformat(),
        }
        producer.send(TOPIC, key=doc["business_id"], value=mensaje)
        enviados += 1
        if enviados % 500 == 0:
            print(f"  {enviados:,}/{total:,} publicadas (mes actual: {mes_actual})")

    producer.flush()
    print(f"Productor terminado: {enviados:,} reseñas publicadas en el topic '{TOPIC}'.")


if __name__ == "__main__":
    main()
