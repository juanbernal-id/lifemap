# LifeMap — reputación, ubicación y supervivencia de negocios (Entrega 1 + Entrega 2)

Curso Big Data · IUE · 2026-2. Área: **Tampa** · Categoría: **Restaurants**
(confirmar con el profe que no se repite con otro equipo).

Construido reutilizando la infraestructura de `bigdata-firms-colombia`
(Docker Compose: Mongo + Spark + Dask + Flask + Jenkins) y la lógica de
MapReduce/balanceo/Kaplan-Meier de `Bigdata-tarea-24oct` (ejercicio de los
discos), adaptadas de "foco de calor" / "disco" a "negocio".

## Cómo levantarlo desde cero

### 0. Credenciales de Kaggle
1. kaggle.com/settings → API → "Create New Token" → descarga `kaggle.json`.
2. `mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json`
3. Para el pipeline: en Jenkins → Administrar Jenkins → Credentials → System →
   Global credentials → Add Credentials → tipo **Username with password**,
   ID **`kaggle-api`**, usuario = `username` de `kaggle.json`, contraseña = `key`.
   El stage `Ingesta (Kaggle)` del Jenkinsfile la usa para descargar el dataset
   (se omite si `data/raw` ya lo tiene). El token nunca va en el repositorio.

### 1. Descargar y filtrar los datos (fuera de Docker, con Python local o el devcontainer)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python ingesta/descargar_kaggle.py     # descarga COMPLETA del Yelp Open Dataset (automatizada)
python ingesta/filtrar_area.py         # recorta a Tampa + Restaurants (permitido por el enunciado
                                        # cuando no se puede procesar el archivo completo)
```

### 2. Levantar la infraestructura
```bash
./levantar.sh
```
Esto levanta Mongo (puerto 27018), API Flask (5051), Spark master/worker
(8091/8092), Dask scheduler/workers (8797) y Jenkins (8090).

### 3. Limpieza + modelo de vida del negocio
```bash
MONGO_URI=mongodb://localhost:27018 python ingesta/limpieza_dask.py
```

### 4. Jobs de Spark (MapReduce, balanceo, sentimiento)
```bash
docker compose exec spark-master bash -c "cd /app && spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 spark_jobs/mapreduce_tasa_cierre.py"
docker compose exec spark-master bash -c "cd /app && spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 spark_jobs/balanceo.py"
docker compose exec spark-master bash -c "cd /app && spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 spark_jobs/sentimiento.py"
```

### 5. Probar la API
```bash
curl http://localhost:5051/health
curl "http://localhost:5051/businesses/near?lat=27.95&lon=-82.46&radio_km=2"
curl "http://localhost:5051/stats/closure?por=categoria_principal"
curl -X POST http://localhost:5051/predict -H "Content-Type: application/json" -d '{"text":"Great food and service!"}'
```

### 6. URL pública (requisito de despliegue, sección 6 del enunciado)
Sin servidor en la nube, se usa un túnel desde el portátil:
```bash
# opción más rápida, sin cuenta:
cloudflared tunnel --url http://localhost:5051
```
URL registrada: **https://receives-cell-gadgets-cradle.trycloudflare.com** (relanzada 2026-11-22, confirmada con /health respondiendo ok; la URL anterior del 7-nov ya no está activa).
Nota: no alcanza a cumplir el "responde desde 48h antes" al 100%, pero sí
responderá durante la ventana de evaluación — se documenta la limitación
en el informe.

### 7. Jenkins
Abrir `http://localhost:8090`, configurar un Pipeline job apuntando a este
repo (Jenkinsfile en la raíz) con credencial `MAP_KEY`... en este proyecto
no aplica MAP_KEY; usar pollSCM (ya configurado en el Jenkinsfile, sin
necesidad de webhook porque Jenkins corre en el portátil, no es alcanzable
desde GitHub).

## Limitaciones documentadas (para el informe)
- El dataset no trae fecha de apertura real: "vida del negocio" se aproxima
  con fecha de primera a última reseña (ver `ingesta/limpieza_dask.py`).
- "Entrada tardía" se aproxima con un umbral de 90 días desde el inicio de
  la ventana de datos del área, no con una fecha de apertura real.
- Modelo de sentimiento entrenado con Spark MLlib (no existe "SentimentStream"
  de un semestre anterior para este equipo); se exporta además una versión
  sklearn equivalente para servir `/predict` sin levantar Spark por request.


## Entrega 2 — streaming, supervivencia y alerta temprana de cierre

Fecha de corte **T = 2021-01-01**: reseñas con `date < T` se usan para
entrenar (sentimiento, riesgo); reseñas con `date >= T` se reservan y las
reproduce el productor como si llegaran en vivo (~33.800 reseñas, hasta
2022-01-19, fin del dataset). Se cambia con la variable `FECHA_CORTE_T`
(formato ISO con offset, ej. `2021-01-01T00:00:00+00:00`).

### 8. Entrenar el modelo de riesgo y las curvas de supervivencia
Con la plataforma de la Entrega 1 ya corriendo (`./levantar.sh`):
```bash
docker run --rm --network lifemap_network -v "$PWD":/app -w /app \
    -e MONGO_URI=mongodb://mongo:27017 lifemap-spark python spark_jobs/riesgo_cierre.py

docker run --rm --network lifemap_network -v "$PWD":/app -w /app \
    -e MONGO_URI=mongodb://mongo:27017 lifemap-spark python spark_jobs/supervivencia.py
```
`riesgo_cierre.py` deja `outputs/modelo_riesgo.pkl` + `outputs/metricas_riesgo.json`,
y guarda un snapshot de features por negocio en `negocios.riesgo_features_t`
(lo que usan las alertas en vivo para no recalcular todo en cada reseña).

### 9. Kafka + demo de streaming
```bash
docker compose up -d kafka kafka-init spark-master spark-worker

# consumidor (dos queries: inferencia+alertas, y ventanas/hotspots) - queda corriendo
docker compose exec -it spark-master spark-submit --master spark://spark-master:7077 \
    spark_jobs/streaming_consumidor.py

# en otra terminal, el productor con reloj acelerado (1 mes de dataset cada 10s)
docker compose --profile streaming up productor
```
Mientras el productor corre, se puede ver en vivo:
```bash
curl http://localhost:5051/streaming/estado
curl "http://localhost:5051/live/hotspots"
curl "http://localhost:5051/alerts?lat=27.95&lon=-82.46&radio_km=5"
curl "http://localhost:5051/risk/near?lat=27.95&lon=-82.46&radio_km=2"
curl "http://localhost:5051/survival/Restaurants"
```
`/health` ahora también reporta Kafka y el retraso (lag) del consumidor.

### 10. Dashboard en Power BI
Se arma en un PC con Power BI Desktop (no en la Mac), conectado a la URL
pública de la API — Power Query → "Obtener datos" → Web → pegar cada URL de
arriba (`Json.Document(Web.Contents("..."))`), actualizar periódicamente
durante la demo. Las 4 vistas mínimas: mapa de hotspots (`/live/hotspots`),
tabla de alertas (`/alerts`), curvas de supervivencia (`/survival/<categoria>`)
y evolución del % negativo (histórico de `ventanas_geohash`, agrupado por
`ventana_inicio`).

## Limitaciones documentadas — Entrega 2
- El modelo de sentimiento de la Entrega 1 ya se entrenó con el dataset
  completo (incluyendo lo que ahora, retroactivamente, es "posterior a T").
  No se reentrena para separar eso, porque la Entrega 1 ya fue calificada;
  la fuga que esto implica es solo para la clasificación de texto, no para
  el modelo de riesgo (E2.6), cuyas variables sí se calculan exclusivamente
  con datos `<= T`.
- Densidad de competidores y tendencia de 6 meses quedan fijas al snapshot
  de entrenamiento (`riesgo_features_t`); las alertas en vivo solo actualizan
  sentimiento medio y conteo de reseñas en tiempo real, no recalculan
  densidad geoespacial ni tendencia en cada micro-lote (quedaría muy costoso
  para el tiempo disponible). Se documenta como simplificación en el informe.
- Las curvas de supervivencia por entrada tardía no estiman un tiempo exacto
  de entrada (el dataset no trae fecha de apertura): se reporta la curva con
  todos los negocios y otra excluyendo los de entrada tardía, en vez de
  inventar una fecha de entrada.
