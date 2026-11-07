import os
import pickle
import socket
from pathlib import Path

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
GIT_COMMIT = os.environ.get("GIT_COMMIT", "dev")
SPARK_MASTER = (os.environ.get("SPARK_MASTER_HOST", "spark-master"), 7077)
DASK_SCHEDULER = (os.environ.get("DASK_SCHEDULER_HOST", "dask-scheduler"), 8786)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
db = client[MONGO_DB]
negocios = db["negocios"]

LIMITE_DEFECTO = 50
LIMITE_MAXIMO = 500
RADIO_DEFECTO_KM = 2
RADIO_MAXIMO_KM = 50
RADIO_TIERRA_KM = 6378.1
TIMEOUT_MS = 30000
MIN_DIAS_NEGOCIO_DEFECTO = 30

PROYECCION = {
    "_id": 0, "business_id": 1, "name": 1, "categories": 1,
    "stars": 1, "review_count": 1, "is_open": 1, "location": 1,
    "dias_negocio": 1, "evento": 1, "censurado": 1, "entrada_tardia": 1,
}


class ParametroInvalido(Exception):
    pass


@app.errorhandler(ParametroInvalido)
def manejar_parametro_invalido(e):
    return jsonify({"error": str(e)}), 400


@app.errorhandler(PyMongoError)
def manejar_error_mongo(e):
    app.logger.error("Error de MongoDB: %s", e)
    return jsonify({"error": "Error consultando MongoDB"}), 503


def leer_float(args, nombre, minimo, maximo, defecto=None):
    valor = args.get(nombre)
    if valor is None:
        if defecto is None:
            raise ParametroInvalido(f"Falta el parametro '{nombre}'")
        return defecto
    try:
        numero = float(valor)
    except ValueError:
        raise ParametroInvalido(f"'{nombre}' debe ser numerico")
    if not (minimo <= numero <= maximo):
        raise ParametroInvalido(f"'{nombre}' debe estar entre {minimo} y {maximo}")
    return numero


def leer_limite(args):
    valor = args.get("limite")
    if valor is None:
        return LIMITE_DEFECTO
    try:
        limite = int(valor)
    except ValueError:
        raise ParametroInvalido("'limite' debe ser un entero")
    if not (1 <= limite <= LIMITE_MAXIMO):
        raise ParametroInvalido(f"'limite' debe estar entre 1 y {LIMITE_MAXIMO}")
    return limite


def serializar(doc):
    return {k: v for k, v in doc.items()}


def validar_poligono(poligono):
    if not isinstance(poligono, dict) or poligono.get("type") != "Polygon":
        raise ParametroInvalido("'poligono' debe ser un GeoJSON de tipo Polygon")
    anillos = poligono.get("coordinates")
    if not isinstance(anillos, list) or not anillos:
        raise ParametroInvalido("'poligono.coordinates' debe ser una lista de anillos")
    for anillo in anillos:
        if not isinstance(anillo, list) or len(anillo) < 4:
            raise ParametroInvalido("Cada anillo necesita al menos 4 puntos")
        if anillo[0] != anillo[-1]:
            raise ParametroInvalido("Cada anillo debe cerrarse (primer y ultimo punto iguales)")


@app.route("/health")
def health():
    estado_mongo = "desconocido"
    try:
        client.admin.command("ping")
        estado_mongo = "conectado"
    except Exception as e:
        estado_mongo = f"error: {e}"

    estado_spark = consultar_spark()
    estado_dask = consultar_dask()

    return jsonify({
        "status": "ok",
        "commit": GIT_COMMIT,
        "db": MONGO_DB,
        "mongo": estado_mongo,
        "servicios": {
            "api": "ok",
            "mongo": estado_mongo,
            "spark": estado_spark,
            "dask": estado_dask,
        },
    })


def puerto_abierto(host_puerto, timeout=1.5):
    """True si el servicio acepta conexiones TCP en su puerto (esta vivo)."""
    try:
        with socket.create_connection(host_puerto, timeout=timeout):
            return True
    except OSError:
        return False


def consultar_spark():
    """Spark master: acepta conexiones en el puerto del cluster (7077)."""
    host, puerto = SPARK_MASTER
    return "activo" if puerto_abierto(SPARK_MASTER) else f"sin respuesta en {host}:{puerto}"


def consultar_dask():
    """Dask scheduler: acepta conexiones en su puerto (8786)."""
    host, puerto = DASK_SCHEDULER
    return "activo" if puerto_abierto(DASK_SCHEDULER) else f"sin respuesta en {host}:{puerto}"


@app.route("/businesses/near")
def businesses_near():
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM)
    limite = leer_limite(request.args)
    categoria = request.args.get("categoria")

    filtro_extra = {"categories": {"$regex": categoria, "$options": "i"}} if categoria else {}

    total = negocios.count_documents(
        {
            "location": {"$geoWithin": {"$centerSphere": [[lon, lat], radio_km / RADIO_TIERRA_KM]}},
            **filtro_extra,
        },
        maxTimeMS=TIMEOUT_MS,
    )

    consulta = {
        "location": {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [lon, lat]},
                "$maxDistance": radio_km * 1000,
            }
        },
        **filtro_extra,
    }
    docs = [
        serializar(d)
        for d in negocios.find(consulta, PROYECCION).limit(limite).max_time_ms(TIMEOUT_MS)
    ]

    return jsonify({
        "centro": {"lat": lat, "lon": lon},
        "radio_km": radio_km,
        "total_en_radio": total,
        "devueltos": len(docs),
        "negocios": docs,
    })


@app.route("/businesses/within", methods=["POST"])
def businesses_within():
    cuerpo = request.get_json(silent=True)
    if not isinstance(cuerpo, dict):
        raise ParametroInvalido("El cuerpo debe ser un JSON con la clave 'poligono'")

    poligono = cuerpo.get("poligono")
    validar_poligono(poligono)
    limite = leer_limite(cuerpo)
    categoria = cuerpo.get("categoria")

    filtro = {"location": {"$geoWithin": {"$geometry": poligono}}}
    if categoria:
        filtro["categories"] = {"$regex": categoria, "$options": "i"}

    try:
        total = negocios.count_documents(filtro, maxTimeMS=TIMEOUT_MS)
        docs = [
            serializar(d)
            for d in negocios.find(filtro, PROYECCION).limit(limite).max_time_ms(TIMEOUT_MS)
        ]
    except OperationFailure as e:
        app.logger.error("Poligono rechazado por MongoDB: %s", e)
        raise ParametroInvalido("MongoDB rechazo el poligono (revisa coordenadas y orden [lon, lat])")

    return jsonify({"total_en_poligono": total, "devueltos": len(docs), "negocios": docs})


@app.route("/zones/summary")
def zones_summary():
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM)

    pipeline = [
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": [lon, lat]},
                "distanceField": "distancia_m",
                "maxDistance": radio_km * 1000,
                "spherical": True,
                "key": "location",
            }
        },
        {
            "$group": {
                "_id": None,
                "negocios": {"$sum": 1},
                "estrellas_promedio": {"$avg": "$stars"},
                "cierres": {"$sum": "$evento"},
                "dias_negocio_total": {"$sum": "$dias_negocio"},
                "ids": {"$push": "$business_id"},
            }
        },
    ]

    resultado = list(negocios.aggregate(pipeline, maxTimeMS=TIMEOUT_MS))
    if not resultado:
        return jsonify({"centro": {"lat": lat, "lon": lon}, "radio_km": radio_km, "negocios": 0})

    r = resultado[0]
    tasa_cierre = (
        (r["cierres"] / (r["dias_negocio_total"] / 365)) * 100
        if r["dias_negocio_total"] else None
    )

    sentimiento = resumen_sentimiento(r["ids"])

    return jsonify({
        "centro": {"lat": lat, "lon": lon},
        "radio_km": radio_km,
        "negocios": r["negocios"],
        "estrellas_promedio": round(r["estrellas_promedio"], 2) if r["estrellas_promedio"] else None,
        "tasa_cierre_pct": round(tasa_cierre, 2) if tasa_cierre is not None else None,
        "sentimiento": sentimiento,
    })


PUNTAJE_SENTIMIENTO = {"positivo": 1, "neutral": 0, "negativo": -1}


def resumen_sentimiento(business_ids):
    """Sentimiento medio de las resenas de los negocios de la zona.

    Puntaje: positivo = +1, neutral = 0, negativo = -1 (etiqueta derivada de
    las estrellas, seccion 4 del enunciado). El medio va de -1 a +1.
    """
    conteos = {
        d["_id"]: d["n"]
        for d in db["reviews"].aggregate(
            [
                {"$match": {"business_id": {"$in": business_ids}}},
                {"$group": {"_id": "$sentimiento_estrellas", "n": {"$sum": 1}}},
            ],
            maxTimeMS=TIMEOUT_MS,
        )
    }
    total = sum(conteos.values())
    if not total:
        return {"resenas": 0, "sentimiento_medio": None, "pct_negativas": None}
    medio = sum(PUNTAJE_SENTIMIENTO.get(k, 0) * n for k, n in conteos.items()) / total
    return {
        "resenas": total,
        "sentimiento_medio": round(medio, 3),
        "pct_negativas": round(conteos.get("negativo", 0) / total * 100, 2),
        "conteo_por_clase": conteos,
    }


@app.route("/stats/closure")
def stats_closure():
    agrupar_por = request.args.get("por", "categoria_principal")
    min_dias = leer_float(request.args, "min_dias_negocio", 0, 100000, MIN_DIAS_NEGOCIO_DEFECTO)

    coleccion = f"tasas_cierre_{agrupar_por}"
    docs = list(
        db[coleccion]
        .find({"dias_negocio": {"$gte": min_dias}}, {"_id": 0})
        .sort("tasa_cierre_pct", -1)
        .max_time_ms(TIMEOUT_MS)
    )
    return jsonify({
        "agrupado_por": agrupar_por,
        "min_dias_negocio": min_dias,
        "nota": "Calculado por el job de MapReduce (spark_jobs/mapreduce_tasa_cierre.py); "
                "exige un minimo de dias-negocio para no comparar grupos pequenos.",
        "total": len(docs),
        "resultados": docs,
    })


_modelo_sentimiento = None


def cargar_modelo_sentimiento():
    global _modelo_sentimiento
    if _modelo_sentimiento is None:
        ruta = Path("/app/outputs/modelo_sentimiento_sklearn.pkl")
        if not ruta.exists():
            raise ParametroInvalido(
                "El modelo de sentimiento todavia no se ha entrenado "
                "(corre spark_jobs/sentimiento.py primero)"
            )
        with open(ruta, "rb") as f:
            _modelo_sentimiento = pickle.load(f)
    return _modelo_sentimiento


@app.route("/predict", methods=["POST"])
def predict():
    cuerpo = request.get_json(silent=True)
    if not isinstance(cuerpo, dict) or not cuerpo.get("text"):
        raise ParametroInvalido("El cuerpo debe ser un JSON con la clave 'text'")

    modelo = cargar_modelo_sentimiento()
    texto = cuerpo["text"]
    prediccion = modelo.predict([texto])[0]
    probabilidades = dict(zip(modelo.classes_, modelo.predict_proba([texto])[0].round(4).tolist()))

    return jsonify({"text": texto, "sentimiento": prediccion, "probabilidades": probabilidades})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
