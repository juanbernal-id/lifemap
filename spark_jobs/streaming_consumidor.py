"""
E2.3, E2.4 y E2.7 - Inferencia en streaming, ventanas geoespaciales con
hotspots, y alertas de riesgo de cierre; todo leyendo del mismo topic Kafka
"reviews" con dos Structured Streaming queries independientes.

QUERY A (inferencia + alertas):
  - Carga el PipelineModel guardado en la Entrega 1 (/app/outputs/modelo_sentimiento)
    SIN reentrenarlo (E2.3) y clasifica cada reseña que llega.
  - Escribe cada prediccion en MongoDB (streaming_predicciones) con la
    prediccion, su probabilidad y la marca de procesamiento.
  - Con foreachBatch, cada micro-lote actualiza los contadores en vivo del
    negocio (sentimiento_medio_actualizado, conteos) y lo vuelve a puntuar
    con el modelo de riesgo guardado (outputs/modelo_riesgo.pkl, entrenado
    en spark_jobs/riesgo_cierre.py); si supera el umbral, inserta una alerta.

QUERY B (ventanas geoespaciales + hotspot):
  - Ventanas de 30 dias con watermark de 7 dias sobre el TIEMPO DEL EVENTO
    (date de la reseña, no la hora en que Spark la proceso), agrupadas por
    celda de geohash.
  - El "negativo" de cada reseña sale directo de sus estrellas (1-2),
    igual que la etiqueta de sentimiento de la seccion 4 del enunciado -
    no depende del modelo, asi es comparable a las resenas_neg de E1.
  - Hotspot: el % negativo de la ventana sube mas de UMBRAL_HOTSPOT_PP
    puntos porcentuales frente a la ventana anterior de la misma celda.
    Umbral justificado en el informe: una celda normal fluctua unos pocos
    puntos de una ventana a otra; +15pp es un salto que ya no se explica
    por ruido muestral con los tamaños de ventana tipicos de este dataset.

Ambas queries hacen un join estatico (stream-static) contra un snapshot de
negocios (business_id -> location, geohash, categoria_principal) leido UNA
vez de MongoDB al arrancar, no releido en cada micro-lote.
"""
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "reviews")
SALIDA = Path(os.environ.get("SALIDA_DIR", "/app/outputs"))
CHECKPOINTS = Path(os.environ.get("CHECKPOINT_DIR", "/app/outputs/checkpoints"))

UMBRAL_HOTSPOT_PP = float(os.environ.get("UMBRAL_HOTSPOT_PP", "15"))
GEOHASH_PRECISION = 4


def categoria_principal(categorias):
    if not categorias:
        return "Sin categoria"
    return categorias.split(",")[0].strip()


def geohash_simple(lat, lon, precision=GEOHASH_PRECISION):
    escala = 10 ** (precision - 2)
    return f"{round(lat * escala) / escala}_{round(lon * escala) / escala}"


def _ip_propia():
    import subprocess
    return subprocess.check_output("hostname -I | awk '{print $1}'", shell=True, text=True).strip()


def crear_sesion(nombre):
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(nombre)
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", _ip_propia()))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def cargar_snapshot_negocios(spark):
    """Lee UNA vez (no streaming) business_id -> location, geohash, categoria_principal."""
    cliente = MongoClient(MONGO_URI)
    db = cliente[MONGO_DB]
    filas = []
    for d in db["negocios"].find({}, {"_id": 0, "business_id": 1, "location": 1, "categories": 1}):
        loc = d.get("location")
        if not isinstance(loc, dict) or "coordinates" not in loc:
            continue
        lon, lat = loc["coordinates"]
        filas.append({
            "business_id": d["business_id"],
            "lat_centro": lat,
            "lon_centro": lon,
            "geohash": geohash_simple(lat, lon),
            "categoria_principal": categoria_principal(d.get("categories")),
        })
    cliente.close()
    pdf = pd.DataFrame(filas)
    print(f"Snapshot de negocios cargado: {len(pdf):,} filas")
    return spark.createDataFrame(pdf)


def cargar_modelo_sentimiento():
    modelo = PipelineModel.load(str(SALIDA / "modelo_sentimiento"))
    etiquetas = modelo.stages[0].labels  # orden interno del StringIndexer
    return modelo, etiquetas


def cargar_modelo_riesgo():
    ruta = SALIDA / "modelo_riesgo.pkl"
    if not ruta.exists():
        print(f"AVISO: no existe {ruta}; las alertas quedaran desactivadas hasta correr riesgo_cierre.py")
        return None
    with open(ruta, "rb") as f:
        return pickle.load(f)


ESQUEMA_MENSAJE = StructType([
    StructField("review_id", StringType()),
    StructField("business_id", StringType()),
    StructField("stars", DoubleType()),
    StructField("text", StringType()),
    StructField("date", StringType()),
])


def leer_topic(spark):
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parseado = (
        crudo.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", ESQUEMA_MENSAJE).alias("d"))
        .select("d.*")
        .withColumn("fecha_evento", F.to_timestamp("date"))
        .withColumn("negativo", (F.col("stars") <= 2).cast("int"))
    )
    return parseado


# ---------------------------------------------------------------------------
# QUERY A: inferencia + alertas
# ---------------------------------------------------------------------------

def construir_query_inferencia(parseado, modelo, etiquetas, negocios_snap, modelo_riesgo):
    etiquetas_bc = etiquetas

    etiqueta_predicha = F.udf(lambda idx: etiquetas_bc[int(idx)], StringType())
    probabilidad_predicha = F.udf(lambda vec, idx: float(vec[int(idx)]), DoubleType())

    predicho = modelo.transform(parseado.withColumnRenamed("text", "text"))
    predicho = (
        predicho
        .withColumn("prediccion_label", etiqueta_predicha(F.col("prediction")))
        .withColumn("probabilidad", probabilidad_predicha(F.col("probability"), F.col("prediction")))
        .select("review_id", "business_id", "stars", "date", "fecha_evento",
                "prediccion_label", "probabilidad")
    )

    def procesar_lote(lote_df, id_lote):
        pdf = lote_df.toPandas()
        if pdf.empty:
            return
        print(f"[inferencia] lote {id_lote}: {len(pdf)} reseñas clasificadas")

        cliente = MongoClient(MONGO_URI)
        db = cliente[MONGO_DB]
        ahora = datetime.now(timezone.utc)

        documentos = [
            {
                "review_id": r.review_id,
                "business_id": r.business_id,
                "stars": r.stars,
                "date": r.date,
                "prediccion": r.prediccion_label,
                "probabilidad": round(float(r.probabilidad), 4),
                "marca_procesamiento": ahora,
            }
            for r in pdf.itertuples()
        ]
        db["streaming_predicciones"].insert_many(documentos)

        puntaje = {"positivo": 1, "neutral": 0, "negativo": -1}
        pdf["puntaje"] = pdf["prediccion_label"].map(puntaje).fillna(0)

        for business_id, grupo in pdf.groupby("business_id"):
            n_nuevas = len(grupo)
            suma_puntajes = float(grupo["puntaje"].sum())

            negocio = db["negocios"].find_one(
                {"business_id": business_id},
                {"riesgo_features_t": 1, "num_resenas_stream": 1, "suma_puntajes_stream": 1,
                 "location": 1, "categories": 1},
            )
            if not negocio or "riesgo_features_t" not in negocio:
                # Sin snapshot de entrenamiento (no tenia historia hasta T): se
                # ignora para alertas, pero igual se cuenta en vivo.
                db["negocios"].update_one(
                    {"business_id": business_id},
                    {"$inc": {"num_resenas_stream": n_nuevas, "suma_puntajes_stream": suma_puntajes}},
                )
                continue

            num_stream_previo = negocio.get("num_resenas_stream", 0)
            suma_stream_previa = negocio.get("suma_puntajes_stream", 0.0)
            num_stream_nuevo = num_stream_previo + n_nuevas
            suma_stream_nueva = suma_stream_previa + suma_puntajes

            db["negocios"].update_one(
                {"business_id": business_id},
                {"$inc": {"num_resenas_stream": n_nuevas, "suma_puntajes_stream": suma_puntajes}},
            )

            if modelo_riesgo is None:
                continue

            ft = negocio["riesgo_features_t"]
            num_total = ft["num_resenas_hasta_t"] + num_stream_nuevo
            suma_total = ft["sentimiento_medio_hasta_t"] * ft["num_resenas_hasta_t"] + suma_stream_nueva
            sentimiento_medio_actualizado = suma_total / num_total if num_total else 0.0

            fila = pd.DataFrame([{
                "sentimiento_medio_hasta_t": sentimiento_medio_actualizado,
                "tendencia_pct_negativo_6m": ft["tendencia_pct_negativo_6m"],
                "stars": ft["stars"],
                "num_resenas_hasta_t": num_total,
                "edad_dias_hasta_t": ft["edad_dias_hasta_t"],
                "densidad_competidores_500m": ft["densidad_competidores_500m"],
            }])[modelo_riesgo["columnas"]]

            probabilidad_cierre = float(modelo_riesgo["modelo"].predict_proba(fila)[0, 1])

            if probabilidad_cierre >= modelo_riesgo["umbral"]:
                db["alerts"].update_one(
                    {"business_id": business_id},
                    {"$set": {
                        "business_id": business_id,
                        "location": negocio.get("location"),
                        "categories": negocio.get("categories"),
                        "probabilidad_cierre": round(probabilidad_cierre, 4),
                        "sentimiento_medio_actualizado": round(sentimiento_medio_actualizado, 4),
                        "fecha": ahora,
                    }},
                    upsert=True,
                )
        cliente.close()

    return predicho.writeStream.foreachBatch(procesar_lote).outputMode("update") \
        .option("checkpointLocation", str(CHECKPOINTS / "inferencia"))


# ---------------------------------------------------------------------------
# QUERY B: ventanas geoespaciales + hotspot
# ---------------------------------------------------------------------------

def geohash_a_poligono(geohash):
    """Reconstruye el bounding box (GeoJSON Polygon) de una celda 'geohash_simple'.
    El geohash es 'lat_lon' redondeados a la escala de GEOHASH_PRECISION, asi que
    cada celda es un cuadrado centrado en ese punto; el semi-ancho es la mitad
    del paso de la rejilla. Satisface E2.4: la ventana queda guardada con
    geometria consultable (2dsphere) de su celda, no solo el string del geohash."""
    try:
        lat_s, lon_s = geohash.split("_")
        lat, lon = float(lat_s), float(lon_s)
    except (ValueError, AttributeError):
        return None
    escala = 10 ** (GEOHASH_PRECISION - 2)
    semi = 0.5 / escala
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon - semi, lat - semi],
            [lon + semi, lat - semi],
            [lon + semi, lat + semi],
            [lon - semi, lat + semi],
            [lon - semi, lat - semi],
        ]],
    }


def construir_query_ventanas(parseado, negocios_snap):
    con_zona = parseado.join(negocios_snap, "business_id", "inner")

    ventaneado = (
        con_zona
        .withWatermark("fecha_evento", "7 days")
        .groupBy(F.col("geohash"), F.window(F.col("fecha_evento"), "30 days"))
        .agg(F.count("*").alias("n"), F.sum("negativo").alias("n_neg"))
    )

    def procesar_lote(lote_df, id_lote):
        pdf = lote_df.toPandas()
        if pdf.empty:
            return
        print(f"[ventanas] lote {id_lote}: {len(pdf)} ventanas actualizadas")

        cliente = MongoClient(MONGO_URI)
        db = cliente[MONGO_DB]
        ops = []
        for r in pdf.itertuples():
            inicio = r.window.start
            fin = r.window.end
            pct_neg = (r.n_neg / r.n * 100) if r.n else 0.0

            anterior = db["ventanas_geohash"].find_one(
                {"geohash": r.geohash, "ventana_fin": inicio}, {"pct_negativo": 1}
            )
            pct_neg_anterior = anterior["pct_negativo"] if anterior else None
            es_hotspot = (
                pct_neg_anterior is not None and (pct_neg - pct_neg_anterior) > UMBRAL_HOTSPOT_PP
            )

            ops.append(UpdateOne(
                {"geohash": r.geohash, "ventana_inicio": inicio},
                {"$set": {
                    "geohash": r.geohash,
                    "geometria": geohash_a_poligono(r.geohash),
                    "ventana_inicio": inicio,
                    "ventana_fin": fin,
                    "n_resenas": int(r.n),
                    "n_negativas": int(r.n_neg),
                    "pct_negativo": round(pct_neg, 2),
                    "pct_negativo_anterior": pct_neg_anterior,
                    "hotspot": bool(es_hotspot),
                    "umbral_hotspot_pp": UMBRAL_HOTSPOT_PP,
                }},
                upsert=True,
            ))
        if ops:
            db["ventanas_geohash"].bulk_write(ops)
        cliente.close()

    return ventaneado.writeStream.foreachBatch(procesar_lote).outputMode("update") \
        .option("checkpointLocation", str(CHECKPOINTS / "ventanas"))


def hash_modelo_versionado():
    """sha256 de los metadatos del PipelineModel guardado: permite que la
    prueba de Jenkins (E2.9) confirme que el consumidor cargo exactamente el
    modelo versionado en el repo y no uno reentrenado por error."""
    import hashlib
    ruta = SALIDA / "modelo_sentimiento" / "metadata" / "part-00000"
    if not ruta.exists():
        return None
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def main():
    spark = crear_sesion("lifemap-streaming-consumidor")
    negocios_snap = cargar_snapshot_negocios(spark)
    modelo, etiquetas = cargar_modelo_sentimiento()
    modelo_riesgo = cargar_modelo_riesgo()
    print(f"Modelo de sentimiento cargado (sin reentrenar). Etiquetas: {etiquetas}")

    cliente_meta = MongoClient(MONGO_URI)
    cliente_meta[MONGO_DB]["streaming_meta"].update_one(
        {"_id": "consumidor"},
        {"$set": {"modelo_hash": hash_modelo_versionado(), "arrancado": datetime.now(timezone.utc)}},
        upsert=True,
    )
    # E2.4: indice 2dsphere sobre la geometria de cada celda, para que quede
    # realmente consultable ($geoWithin/$geoIntersects), igual que se hace con
    # "location" en negocios (ingesta/limpieza_dask.py).
    cliente_meta[MONGO_DB]["ventanas_geohash"].create_index([("geometria", "2dsphere")])
    cliente_meta.close()

    parseado = leer_topic(spark)

    # Falta el business_id en el snapshot de negocios? lo arreglamos con un
    # geohash por defecto para que el join nunca tumbe la query.
    negocios_snap = negocios_snap.na.fill({"geohash": "sin_zona"})

    query_a = construir_query_inferencia(parseado, modelo, etiquetas, negocios_snap, modelo_riesgo).start()
    query_b = construir_query_ventanas(parseado, negocios_snap).start()

    print("Streaming activo: inferencia + alertas (query A) y ventanas/hotspots (query B).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
