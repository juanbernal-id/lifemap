import os
import pickle
import socket
from pathlib import Path

from datetime import datetime, timezone

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

app = Flask(__name__)

@app.after_request
def _agregar_cors(resp):
    # CORS abierto de solo lectura, para que el dashboard (archivo local o pagina estatica)
    # pueda hacer fetch() a estos endpoints desde el navegador durante la demo en clase.
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
GIT_COMMIT = os.environ.get("GIT_COMMIT", "dev")
SPARK_MASTER = (os.environ.get("SPARK_MASTER_HOST", "spark-master"), 7077)
DASK_SCHEDULER = (os.environ.get("DASK_SCHEDULER_HOST", "dask-scheduler"), 8786)
KAFKA_BROKER = (os.environ.get("KAFKA_BROKER_HOST", "kafka"), 9092)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
db = client[MONGO_DB]
negocios = db["negocios"]
ventanas_geohash = db["ventanas_geohash"]
alerts_col = db["alerts"]
supervivencia_categoria = db["supervivencia_categoria"]

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
    estado_kafka = consultar_kafka()
    retraso_consumidor = consultar_retraso_consumidor()

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
            "kafka": estado_kafka,
        },
        "kafka_consumer_lag": retraso_consumidor,
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


def consultar_kafka():
    """Kafka broker: acepta conexiones en su puerto (9092)."""
    host, puerto = KAFKA_BROKER
    return "activo" if puerto_abierto(KAFKA_BROKER) else f"sin respuesta en {host}:{puerto}"


def consultar_retraso_consumidor():
    """Retraso (lag) del consumidor Structured Streaming: diferencia entre la
    marca de procesamiento mas reciente guardada en Mongo y la hora actual.
    Si no hay documentos (el consumidor no ha corrido o no hay trafico
    reciente), se informa null en vez de fallar el healthcheck."""
    try:
        ultimo = db["streaming_predicciones"].find_one(
            {}, {"marca_procesamiento": 1, "_id": 0}, sort=[("marca_procesamiento", -1)]
        )
        if not ultimo or not ultimo.get("marca_procesamiento"):
            return None
        from datetime import datetime, timezone
        marca = ultimo["marca_procesamiento"]
        if marca.tzinfo is None:
            marca = marca.replace(tzinfo=timezone.utc)
        segundos = (datetime.now(timezone.utc) - marca).total_seconds()
        return round(segundos, 1)
    except Exception:
        return None


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
                "resenas_pos": {"$sum": "$resenas_pos"},
                "resenas_neu": {"$sum": "$resenas_neu"},
                "resenas_neg": {"$sum": "$resenas_neg"},
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

    sentimiento = resumen_sentimiento({
        "positivo": r.get("resenas_pos") or 0,
        "neutral": r.get("resenas_neu") or 0,
        "negativo": r.get("resenas_neg") or 0,
    })

    return jsonify({
        "centro": {"lat": lat, "lon": lon},
        "radio_km": radio_km,
        "negocios": r["negocios"],
        "estrellas_promedio": round(r["estrellas_promedio"], 2) if r["estrellas_promedio"] else None,
        "tasa_cierre_pct": round(tasa_cierre, 2) if tasa_cierre is not None else None,
        "sentimiento": sentimiento,
    })


PUNTAJE_SENTIMIENTO = {"positivo": 1, "neutral": 0, "negativo": -1}


def resumen_sentimiento(conteos):
    """Sentimiento medio de las resenas de los negocios de la zona.

    Los conteos por clase estan precalculados en cada negocio (resenas_pos,
    resenas_neu, resenas_neg) a partir de la etiqueta derivada de las estrellas
    (seccion 4 del enunciado). Puntaje: positivo = +1, neutral = 0,
    negativo = -1; el medio va de -1 a +1.
    """
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


GEOHASH_PRECISION = 4


def geohash_a_centro(celda):
    lat_str, lon_str = celda.split("_")
    return float(lat_str), float(lon_str)


@app.route("/live/hotspots")
def live_hotspots():
    """E2.4/E2.8 - celdas marcadas como hotspot en su ventana mas reciente,
    opcionalmente filtradas por radio alrededor de un punto."""
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM) if lat else None
    solo_hotspots = request.args.get("solo_hotspots", "true").lower() != "false"

    pipeline = [
        {"$sort": {"geohash": 1, "ventana_inicio": -1}},
        {"$group": {"_id": "$geohash", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    if solo_hotspots:
        pipeline.append({"$match": {"hotspot": True}})

    docs = list(ventanas_geohash.aggregate(pipeline, maxTimeMS=TIMEOUT_MS))
    for d in docs:
        d.pop("_id", None)
        d["centro"] = dict(zip(["lat", "lon"], geohash_a_centro(d["geohash"])))

    if lat is not None and lon is not None:
        lat_f = leer_float(request.args, "lat", -90, 90)
        lon_f = leer_float(request.args, "lon", -180, 180)
        docs = [
            d for d in docs
            if _distancia_km(lat_f, lon_f, d["centro"]["lat"], d["centro"]["lon"]) <= radio_km
        ]

    return jsonify({"total": len(docs), "umbral_hotspot_pp": UMBRAL_HOTSPOT_PP, "ventanas": docs})


def _distancia_km(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 2 * RADIO_TIERRA_KM * asin(sqrt(a))


@app.route("/alerts")
def alerts_get():
    """E2.7/E2.8 - alertas activas por radio."""
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM)

    filtro = {
        "location": {"$geoWithin": {"$centerSphere": [[lon, lat], radio_km / RADIO_TIERRA_KM]}},
    }
    docs = [
        {k: v for k, v in d.items() if k != "_id"}
        for d in alerts_col.find(filtro, maxTimeMS=TIMEOUT_MS).sort("probabilidad_cierre", -1)
    ]
    nombres = {
        n["business_id"]: n.get("name")
        for n in negocios.find({"business_id": {"$in": [d["business_id"] for d in docs]}},
                               {"business_id": 1, "name": 1, "_id": 0})
    }
    for d in docs:
        d["name"] = nombres.get(d["business_id"])
    return jsonify({"centro": {"lat": lat, "lon": lon}, "radio_km": radio_km, "total": len(docs), "alertas": docs})


@app.route("/alerts", methods=["POST"])
def alerts_post():
    """Mismo endpoint, por poligono GeoJSON en vez de radio."""
    cuerpo = request.get_json(silent=True)
    if not isinstance(cuerpo, dict):
        raise ParametroInvalido("El cuerpo debe ser un JSON con la clave 'poligono'")
    poligono = cuerpo.get("poligono")
    validar_poligono(poligono)

    try:
        docs = [
            {k: v for k, v in d.items() if k != "_id"}
            for d in alerts_col.find(
                {"location": {"$geoWithin": {"$geometry": poligono}}}, maxTimeMS=TIMEOUT_MS
            ).sort("probabilidad_cierre", -1)
        ]
    except OperationFailure as e:
        app.logger.error("Poligono rechazado por MongoDB: %s", e)
        raise ParametroInvalido("MongoDB rechazo el poligono (revisa coordenadas y orden [lon, lat])")

    return jsonify({"total": len(docs), "alertas": docs})



def _limpiar_json(obj):
    """Reemplaza Infinity/-Infinity/NaN (validos en Python/Mongo pero NO en JSON
    estandar) por None, para que el navegador pueda hacer JSON.parse() sin tronar."""
    if isinstance(obj, dict):
        return {k: _limpiar_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_limpiar_json(v) for v in obj]
    if isinstance(obj, float) and (obj != obj or obj in (float("inf"), float("-inf"))):
        return None
    return obj

@app.route("/survival")
def survival_lista():
    """Categorias que tienen curva KM (para el selector del dashboard)."""
    docs = list(supervivencia_categoria.find({}, {"_id": 0, "categoria_principal": 1,
                                                  "todos.n": 1, "todos.mediana_dias": 1}))
    docs.sort(key=lambda d: -((d.get("todos") or {}).get("n") or 0))
    return jsonify(_limpiar_json({"total": len(docs), "categorias": docs}))


@app.route("/live/evolucion")
def live_evolucion():
    """E2.8 - evolucion del % de resenas negativas por ventana de 30 dias
    (tiempo del evento), sumando todas las celdas de geohash."""
    pipeline = [
        {"$group": {"_id": "$ventana_inicio",
                    "n_resenas": {"$sum": "$n_resenas"},
                    "n_negativas": {"$sum": "$n_negativas"},
                    "celdas": {"$sum": 1},
                    "hotspots": {"$sum": {"$cond": ["$hotspot", 1, 0]}}}},
        {"$sort": {"_id": 1}},
    ]
    serie = []
    for d in ventanas_geohash.aggregate(pipeline, maxTimeMS=TIMEOUT_MS):
        serie.append({
            "ventana_inicio": d["_id"],
            "n_resenas": d["n_resenas"],
            "pct_negativo": round(d["n_negativas"] / d["n_resenas"] * 100, 2) if d["n_resenas"] else 0.0,
            "celdas": d["celdas"],
            "hotspots": d["hotspots"],
        })
    return jsonify({"total": len(serie), "serie": serie})


@app.route("/survival/<categoria>")
def survival_categoria(categoria):
    """E2.5/E2.8 - curva de Kaplan-Meier por categoria principal."""
    doc = supervivencia_categoria.find_one(
        {"categoria_principal": {"$regex": f"^{categoria}$", "$options": "i"}}, {"_id": 0}
    )
    if not doc:
        raise ParametroInvalido(
            f"No hay curva de supervivencia para la categoria '{categoria}' "
            "(no existe o no alcanza el minimo de negocios del grupo)"
        )
    return jsonify(_limpiar_json(doc))


UMBRAL_HOTSPOT_PP = 15


_modelo_riesgo = None


def cargar_modelo_riesgo():
    global _modelo_riesgo
    if _modelo_riesgo is None:
        ruta = Path("/app/outputs/modelo_riesgo.pkl")
        if not ruta.exists():
            raise ParametroInvalido(
                "El modelo de riesgo todavia no se ha entrenado "
                "(corre spark_jobs/riesgo_cierre.py primero)"
            )
        with open(ruta, "rb") as f:
            _modelo_riesgo = pickle.load(f)
    return _modelo_riesgo


@app.route("/risk/near")
def risk_near():
    """E2.6/E2.8 - puntua con el modelo de riesgo los negocios alrededor de un
    punto ($geoNear), incluyendo lo que han actualizado las alertas en vivo."""
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM)
    limite = leer_limite(request.args)
    categoria = request.args.get("categoria")

    modelo_riesgo = cargar_modelo_riesgo()

    pipeline = [
        {"$geoNear": {
            "near": {"type": "Point", "coordinates": [lon, lat]},
            "distanceField": "distancia_m",
            "maxDistance": radio_km * 1000,
            "spherical": True,
            "key": "location",
        }},
        {"$match": {"riesgo_features_t": {"$exists": True}}},
    ]
    if categoria:
        pipeline.append({"$match": {"categories": {"$regex": categoria, "$options": "i"}}})
    pipeline.append({"$limit": limite})

    resultados = []
    for d in negocios.aggregate(pipeline, maxTimeMS=TIMEOUT_MS):
        ft = d["riesgo_features_t"]
        num_stream = d.get("num_resenas_stream", 0)
        suma_stream = d.get("suma_puntajes_stream", 0.0)
        num_total = ft["num_resenas_hasta_t"] + num_stream
        suma_total = ft["sentimiento_medio_hasta_t"] * ft["num_resenas_hasta_t"] + suma_stream
        sentimiento_actualizado = suma_total / num_total if num_total else 0.0

        fila = [[
            sentimiento_actualizado,
            ft["tendencia_pct_negativo_6m"],
            ft["stars"],
            num_total,
            ft["edad_dias_hasta_t"],
            ft["densidad_competidores_500m"],
        ]]
        probabilidad = float(modelo_riesgo["modelo"].predict_proba(fila)[0][1])

        resultados.append({
            "business_id": d["business_id"],
            "name": d.get("name"),
            "lat": d["location"]["coordinates"][1],
            "lon": d["location"]["coordinates"][0],
            "distancia_m": round(d["distancia_m"], 1),
            "probabilidad_cierre": round(probabilidad, 4),
            "sentimiento_actualizado": round(sentimiento_actualizado, 4),
            "resenas_desde_t": num_total,
        })

    resultados.sort(key=lambda r: r["probabilidad_cierre"], reverse=True)
    return jsonify({"centro": {"lat": lat, "lon": lon}, "radio_km": radio_km, "total": len(resultados),
                     "umbral_alerta": modelo_riesgo["umbral"], "negocios": resultados})


def _hash_modelo_sentimiento():
    import hashlib
    ruta = Path("/app/outputs/modelo_sentimiento/metadata/part-00000")
    if not ruta.exists():
        return None
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


@app.route("/streaming/estado")
def streaming_estado():
    """Para la prueba de humo de Jenkins (E2.9): confirma que el consumidor
    de streaming cargo el modelo versionado (no uno reentrenado) y que ya
    escribio predicciones en MongoDB."""
    meta = db["streaming_meta"].find_one({"_id": "consumidor"}, {"_id": 0})
    return jsonify({
        "predicciones_totales": db["streaming_predicciones"].count_documents({}),
        "alertas_totales": alerts_col.count_documents({}),
        "modelo_hash_api": _hash_modelo_sentimiento(),
        "modelo_hash_consumidor": meta.get("modelo_hash") if meta else None,
        "consumidor_arrancado": meta.get("arrancado") if meta else None,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
