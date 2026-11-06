# LifeMap — Entrega 1 (reputación, ubicación y supervivencia de negocios)

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
Registrar la URL que imprime en este README antes de la sustentación.
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
