# LifeMap — Informe técnico, Entrega 1

**Curso:** Big Data · IUE · 2026-2
**Área metropolitana:** Tampa · **Categoría:** Restaurants
**Equipo:** Juan Pablo Bernal Echeverry
**Repositorio:** (pegar URL de GitHub)
**URL desplegada:** https://diagram-located-derby-cases.trycloudflare.com (registrada 2026-11-06 12:41 hora Bogotá / 17:41 UTC)

> NOTA: completar cada sección con las cifras y capturas reales una vez
> corridos los pasos del README. Los placeholders están marcados con `[ ]`.

## 1. Arquitectura

Pipeline: Yelp Open Dataset (Kaggle) → filtrado por área/categoría →
Dask (limpieza + vida del negocio) → MongoDB (GeoJSON + 2dsphere) →
Spark (MapReduce de tasas de cierre, balanceo, sentimiento) → API Flask →
Docker Compose → Jenkins (build/test/deploy).

Reutiliza la infraestructura de `bigdata-firms-colombia` (Docker Compose,
patrón de limpieza con Dask, API geoespacial) y la lógica de MapReduce,
balanceo y Kaplan-Meier de `Bigdata-tarea-24oct` (ejercicio "¿Cuánto viven
los discos?"), adaptadas de foco de calor / disco a negocio.

[ ] Insertar diagrama de servicios (docker-compose.yml) con puertos.

## 2. Decisiones de limpieza

- Negocios y reseñas filtrados a Tampa + Restaurants antes de limpiar
  (la descarga completa del dataset SÍ es automática, ver
  `ingesta/descargar_kaggle.py`).
- [ ] Total de negocios leídos / conservados: ___
- [ ] Total de reseñas leídas / conservadas: ___
- Se descartan negocios sin coordenadas válidas o fuera de rango.
- Se descartan reseñas sin texto o sin fecha válida.

## 3. Construcción de la vida de cada negocio

- `dias_negocio` = última reseña − primera reseña (aproximación; el
  dataset no trae fecha real de apertura/cierre — limitación documentada).
- `evento` = 1 si `is_open = 0` (cierre observado); `censurado` = 1 si
  `is_open = 1` (sigue abierto, solo se sabe que vivió hasta el corte).
- `entrada_tardia` ≈ negocios cuya primera reseña cae en los primeros 90
  días de la ventana de datos del área (no se puede descartar que ya
  existieran antes; aproximación explícita, no una fecha real de apertura).
- [ ] Cierres observados: ___ / Censurados (abiertos): ___ / Entrada
  tardía (aprox.): ___

## 4. Modelo de sentimiento (E1.6)

No existe "SentimentStream" de un semestre anterior para este equipo, así
que se entrenó un pipeline nuevo con Spark MLlib: Tokenizer → StopWordsRemover
→ HashingTF/IDF → Naive Bayes y regresión logística. Partición
entrenamiento/prueba POR NEGOCIO (no por reseña), para evitar que el mismo
negocio aparezca en ambos lados.

- [ ] Distribución de clases (positivo/neutral/negativo): ___ (desbalanceada)
- [ ] Naive Bayes — F1 / precision / recall: ___
- [ ] Regresión logística — F1 / precision / recall: ___
- [ ] Matriz de confusión de ambos modelos (pegar salida de
  `spark_jobs/sentimiento.py`)
- Modelo ganador guardado en `outputs/modelo_sentimiento` (Spark) y
  `outputs/modelo_sentimiento_sklearn.pkl` (servido por `/predict`).

## 5. Tasas de cierre (MapReduce, E1.7)

Cálculo con `reduceByKey` sobre `(categoría_principal | geohash, (días_negocio,
evento))`, validado contra `groupBy` de DataFrames (0 discrepancias).

- [ ] Top 5 categorías por tasa de cierre anualizada (mínimo de días-negocio
  aplicado): ___
- [ ] Top 5 celdas geográficas por tasa de cierre: ___

## 6. Balanceo de carga (E1.8)

- [ ] Tabla de tiempos (2/8/32/200 particiones, 3 repeticiones, promedio y
  desviación): pegar de `outputs/benchmarks.json`
- [ ] Sesgo con reparto por categoría vs. reparto uniforme (max/min filas
  por partición): ___
- Interpretación: [ ] explicar por qué el tiempo sube o baja entre
  configuraciones y de dónde viene el sesgo.

## 7. Consultas geoespaciales y API (E1.9)

Endpoints: `/health`, `/businesses/near`, `/businesses/within`,
`/zones/summary`, `/stats/closure`, `/predict`.

- [ ] Ejemplo de respuesta de cada endpoint (pegar curl + JSON).

## 8. Despliegue (E1.1 – E1.3)

- Orquestación: `./levantar.sh` levanta Mongo, API, Spark, Dask y Jenkins
  con un solo comando (probado en equipo limpio: [ ] sí / no).
- URL pública: [ ] pegar URL de Cloudflare Tunnel / ngrok y hora en que
  quedó respondiendo.
- Jenkins: pipeline con `pollSCM` (sin webhook porque Jenkins corre en el
  portátil del desarrollador, no es alcanzable desde GitHub).
- **Limitación conocida:** la URL no alcanza a responder desde 48h antes
  de la entrega (se montó el mismo día), pero sí responde durante la
  ventana de evaluación.

## 9. Limitaciones generales

- Vida del negocio y entrada tardía son aproximaciones a partir de fechas
  de reseñas, no de fechas reales de apertura/cierre (el dataset no las trae).
- Equipo de una sola persona: alcance recortado a un área y categoría,
  trabajando con un subconjunto filtrado del dataset (descarga completa sí
  automatizada, como permite el enunciado).
- No existía "SentimentStream" previo para este equipo; el modelo de
  sentimiento se construyó desde cero para esta entrega.
