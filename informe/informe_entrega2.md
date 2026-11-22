# LifeMap — Informe técnico Entrega 2

Streaming, supervivencia y alerta temprana de cierre · 2026-11-22 · Juan Pablo Bernal Echeverry

LifeMap es el proyecto final del curso de Big Data (IUE), construido por Juan Pablo Bernal Echeverry sobre el dataset Yelp Open Dataset (área metropolitana de Tampa, categoría Restaurants). La Entrega 1 cubrió reputación, ubicación y supervivencia de negocios con procesamiento batch (Dask + Spark + MongoDB + API Flask, orquestado con Docker Compose y Jenkins). La Entrega 2, descrita en este informe, añade **streaming en tiempo real con Kafka**, un **modelo de riesgo de cierre** entrenado solo con información anterior a una fecha de corte T, **curvas de supervivencia Kaplan-Meier** por categoría y zona, y **alertas tempranas** calculadas sobre el flujo de reseñas en vivo — sin reemplazar ni modificar la plataforma ya calificada de la Entrega 1.

## 1. Arquitectura general (Entrega 1 + Entrega 2)

La Entrega 2 añade tres piezas nuevas a los siete servicios de Docker Compose de la Entrega 1 (`mongo`, `api`, `spark-master`, `spark-worker`, `dask-scheduler`, `dask-worker` x2, `jenkins`): un broker de **Kafka** en modo KRaft, un job de inicialización de tópico (`kafka-init`) y un **productor** que reproduce reseñas reservadas como si llegaran en vivo.

| Servicio | Rol | Entrega |
| --- | --- | --- |
| mongo | Negocios, reseñas, predicciones y ventanas en tiempo real (índice 2dsphere) | E1 |
| dask-scheduler / dask-worker | Limpieza e ingesta distribuida, cálculo de vida del negocio | E1 |
| spark-master / spark-worker | MapReduce de cierre, balanceo de carga, sentimiento, y ahora riesgo de cierre + supervivencia + streaming | E1 / E2 |
| api | Flask; expone endpoints batch (E1) y en vivo (E2) | E1 / E2 |
| jenkins | CI/CD: build, ingesta, test, deploy; ahora con prueba de humo de streaming | E1 / E2 |
| kafka / kafka-init | Broker (KRaft, sin Zookeeper) y creación del tópico `reviews` (3 particiones) | E2 |
| productor | Publica reseñas con fecha ≥ T al tópico `reviews`, con reloj acelerado | E2 |

**Flujo de datos:**

1. (E1, batch) Kaggle → Dask (limpieza) → Mongo → Spark (sentimiento, cierre, balanceo) → API.
2. (E2, streaming) El `productor` lee de Mongo las reseñas con `date >= T` y las publica en Kafka → el consumidor de Spark Structured Streaming las clasifica con el modelo de sentimiento ya entrenado, las une (join streaming-static) con el snapshot de negocios para obtener `geohash`/categoría, actualiza `streaming_predicciones` y dispara **alertas** cuando el riesgo supera el umbral; en paralelo, una segunda query agrega por ventanas de 30 días y `geohash` para detectar **hotspots** de reputación negativa.
3. El modelo de riesgo de cierre (`riesgo_cierre.py`) y las curvas de supervivencia (`supervivencia.py`) corren como jobs batch de Spark, usando solo datos con `date < T`, y dejan sus resultados (`modelo_riesgo.pkl`, `riesgo_features_t`, curvas KM) listos para que la API y el consumidor los lean.

## 2. Streaming con Kafka: decisiones de diseño

**Fecha de corte T = 2021-01-01.** Se eligió revisando la distribución mensual de reseñas del dataset: deja suficiente historia antes de T para entrenar (sentimiento ya entrenado en E1, riesgo de cierre, supervivencia) y suficiente volumen después de T (≈33.800 reseñas hasta el fin del dataset, 2022-01-19) para que la demo de streaming tenga datos reales que reproducir.

**Kafka en modo KRaft** (imagen oficial `apache/kafka:3.8.0`), sin Zookeeper, con un tópico `reviews` de 3 particiones. Se usó un `CLUSTER_ID` generado como UUID base64url válido — el paso de formateo de almacenamiento de la imagen rechaza un string arbitrario.

**Productor con reloj acelerado:** en vez de reproducir las reseñas en su cadencia real (que tomaría meses), el productor pausa `SEGUNDOS_POR_MES` segundos (configurable — 10s en demo manual, 0s en la prueba de Jenkins) cada vez que cruza un mes del dataset, publicando cada reseña como un evento JSON con su `business_id`, `stars`, `text` y `date`.

**Consumidor con dos queries de Spark Structured Streaming** sobre el mismo tópico:

- *Inferencia + alertas:* aplica el `PipelineModel` de sentimiento de la Entrega 1 directamente sobre el stream (funciona sin `foreachBatch`, porque ninguna etapa del pipeline agrega), hace un join streaming-static contra el snapshot de negocios para traer `geohash`/categoría, y escribe en Mongo (`streaming_predicciones`, `alerts`) usando `foreachBatch` (Structured Streaming no tiene sink nativo de Mongo).
- *Ventanas y hotspots:* usa `withWatermark` (7 días) + ventanas tumbling de 30 días sobre `fecha_evento`, agrupando por `geohash`, para detectar zonas con un alza sostenida de reseñas negativas (`ventanas_geohash`). Una celda es *hotspot* cuando su % de reseñas negativas sube más de **15 puntos porcentuales** frente a la ventana anterior de la misma celda (`UMBRAL_HOTSPOT_PP=15`, configurable por variable de entorno) — umbral elegido para distinguir un repunte real de la variación normal entre meses.

## 3. Modelo de riesgo de cierre

**Construcción de la etiqueta y las features:** para cada negocio, todas las variables se calculan exclusivamente con información `<= T` (sentimiento medio, tendencia de % negativo en los últimos 6 meses, `stars`, número de reseñas, edad del negocio en días hasta T, densidad de competidores en 500m vía `$geoNear`). De 2.967 negocios, 1.750 tienen historia y etiqueta definida (1.217 excluidos), con una distribución fuertemente desbalanceada (1.675 negativos / 75 positivos).

**Split reproducible:** el primer intento usó `hash()` nativo de Python sobre `business_id`, que es aleatorio por proceso (semilla no fija) y producía un split distinto en cada corrida. Se corrigió usando un hash estable (`hashlib.md5`), dejando 1.406 negocios de entrenamiento y 344 de prueba de forma reproducible.

**Métricas finales** (RandomForest con `class_weight="balanced"`):

| Métrica | Valor |
| --- | --- |
| AUC-ROC | 0.6065 |
| AUC-PR | 0.0565 (línea base: 0.0429) |
| Recall @ umbral 0.15 | 0.80 |
| Matriz de confusión | [[118, 211], [3, 12]] |

**Por qué el umbral 0.15 y no 0.5:** con el umbral por defecto (0.5), el desbalance severo (75 de 1.750 positivos) hace que el modelo nunca prediga la clase positiva (recall = 0), inútil para una demo de alertas en vivo. Bajar el umbral a 0.15 sube el recall a 0.80 a costa de más falsos positivos — una decisión explícita documentada como trade-off, no un resultado casual.

**Importancia de variables:** sentimiento medio hasta T (0.2153) y número de reseñas hasta T (0.191) son las más influyentes, seguidas de edad del negocio (0.1865), densidad de competidores (0.1729), tendencia de % negativo (0.177) y `stars` (0.0573).

## 4. Supervivencia (Kaplan-Meier)

Se estimaron curvas de Kaplan-Meier sobre 2.967 negocios, manejando correctamente la censura por la derecha (negocios con `is_open=1`, que entran como censurados en lugar de "evento no ocurrido").

Cada curva reporta, además de la mediana de supervivencia, el **número de negocios en riesgo** (`en_riesgo`) en cada punto de tiempo, calculado con `kmf.event_table`, para que la mediana no se lea sin el tamaño de muestra que queda en cada tramo de la curva.

**Por categoría** (20 curvas): por ejemplo, Restaurants (n=851) tiene una mediana de supervivencia de 4.956 días; varias categorías con pocos negocios no alcanzan la mediana observada (`mediana=inf`), documentado como limitación de tamaño de muestra, no como error de cálculo.

**Por zona geográfica** (15 curvas, zonas con ≥30 negocios, consultadas vía `$geoWithin` con un polígono rectangular por celda de geohash).

**Entrada tardía:** el dataset no trae una fecha real de apertura, así que en vez de inventar una fecha de entrada para los negocios que aparecen tarde en la ventana observada, se reportan **dos curvas paralelas por grupo** — "todos" y "sin entrada tardía" — tratando la segunda como la corregida, dejando explícito qué supuesto se evita.

## 5. API, CI/CD y validación en Jenkins

**Nuevos endpoints:** `GET /live/hotspots`, `GET/POST /alerts`, `GET /survival/<categoria>`, `GET /risk/near` (con `$geoNear`, scoring en vivo contra el modelo de riesgo), `GET /streaming/estado`. `/health` ahora también reporta el estado de Kafka y el *lag* del consumidor.

**Etapa nueva en Jenkins — "Prueba de streaming (E2.9)":** levanta Kafka y Spark, lanza el consumidor en segundo plano, corre el productor con un lote corto (80 mensajes, sin pausas) y verifica contra la API que las predicciones llegaron a Mongo con el modelo versionado correcto (no reentrenado), vía `pytest test_streaming.py`.

**Validación real:** la corrida automática de Jenkins (disparada por `pollSCM` al hacer push) ejecutó el pipeline completo — Build, Ingesta, Test (11/11), *Prueba de streaming* (consumidor arriba, 80 reseñas publicadas, 2/2 pruebas), Deploy — y terminó en `Finished: SUCCESS`. Una prueba manual adicional, fuera de Jenkins, confirmó 80 predicciones y 47 alertas generadas en vivo.

**Corrección de aislamiento de pruebas:** se detectó que `test_streaming.py`, al estar empaquetado en la imagen de pruebas, también se ejecutaba (y pasaba por casualidad, gracias a datos residuales de una corrida manual previa) dentro de la etapa genérica *Test*. Se corrigió excluyéndolo ahí (`pytest -v --ignore=test_streaming.py`) para que solo se ejecute, de forma intencional, en su etapa dedicada.

**Validación end-to-end (2026-11-22):** se corrió el pipeline completo de punta a punta — productor (33.803 reseñas) → Kafka → consumidor Spark → Mongo — generando 34.043 predicciones, 3.111 ventanas geohash y 1.145 alertas reales. En el camino se diagnosticó y corrigió un bug de infraestructura: el *healthcheck* de Docker para `spark-worker` solo verifica que el proceso esté vivo, no que siga conectado al master; el worker llevaba 2 días desconectado en silencio, dejando la aplicación de streaming en estado `WAITING` con 0 cores asignados. Se resolvió con `docker compose restart spark-worker`, tras lo cual el master reasignó recursos automáticamente y la aplicación pasó a `RUNNING` sin necesidad de reenviarla. El túnel público (Cloudflare Tunnel) y el pipeline de Jenkins (build más reciente en verde) se reconfirmaron funcionando el mismo día. Los datos generados por esta validación (predicciones, ventanas, alertas y el checkpoint de Spark) se limpiaron de Mongo y disco después de documentarlos, para que la sustentación en vivo arranque desde cero.

## 6. Dashboard en Power BI

El dashboard conectado a la API (`scripts/export_powerbi.py`, que exporta un snapshot a `/app/demo/lifemap_powerbi.xlsx` con 4 hojas: Negocios, Alertas, Supervivencia, Evolución) tiene las cuatro visualizaciones que pide el enunciado:

- **Mapa de hotspots**: negocios ubicados en el mapa, coloreados por probabilidad de cierre / sentimiento.
- **Tabla de alertas activas**: alertas vigentes con nombre, categoría, probabilidad y fecha.
- **Curvas de supervivencia**: Kaplan-Meier por categoría principal.
- **Evolución del % de reseñas negativas**: serie temporal agregada por celda de geohash.

Se actualiza re-ejecutando el export dentro de la red de Docker Compose y refrescando el dataset en Power BI Service; en la sustentación se corre después de que el productor publique nuevas reseñas, para mostrar el refresco en vivo.

## 7. Limitaciones documentadas

- El modelo de sentimiento de la Entrega 1 se entrenó con el dataset completo, incluyendo lo que ahora, retroactivamente, es "posterior a T". No se reentrena para separar eso, porque la Entrega 1 ya fue calificada; esa fuga afecta solo la clasificación de texto, no el modelo de riesgo (E2.6), cuyas variables sí se calculan exclusivamente con datos `<= T`.
- La densidad de competidores y la tendencia de 6 meses quedan fijas al snapshot de entrenamiento (`riesgo_features_t`); las alertas en vivo solo actualizan sentimiento medio y conteo de reseñas en tiempo real, no recalculan densidad geoespacial ni tendencia en cada micro-lote (sería demasiado costoso para el tiempo disponible).
- Las curvas de supervivencia por entrada tardía no estiman una fecha exacta de entrada (el dataset no la trae): se reporta la curva con todos los negocios y otra excluyéndolos, en vez de inventar una fecha.
- El despliegue sigue corriendo desde el Mac de Juan con un túnel (Cloudflare Tunnel/ngrok), no en un servidor separado — la misma limitación ya documentada en la Entrega 1.

## 8. Reflexión: continuidad con otros trabajos del semestre

La infraestructura de Docker/Spark/Dask/Mongo/Jenkins de LifeMap se reutilizó de `bigdata-firms-colombia` (otro proyecto del semestre), y la lógica de MapReduce, balanceo de carga y Kaplan-Meier venía de `Bigdata-tarea-24oct` ("¿Cuánto viven los discos?") — ya documentado en la Entrega 1 como continuidad de aprendizaje, no partir de cero.

Para la Entrega 2, el mismo patrón se repitió con el streaming: el problema clásico de redes de Spark en Docker (el driver anunciándose como `localhost` cuando corre en un contenedor efímero, por lo que los ejecutores nunca logran registrarse) ya se había resuelto en `spark_jobs/comun.py` (`_ip_propia()`) para los jobs batch de la Entrega 1, y la misma solución se aplicó al nuevo `streaming_consumidor.py` en cuanto apareció el mismo síntoma (el job se colgaba en silencio, sin ningún progreso). De forma similar, el error de workspace compartido entre Jenkins y el directorio de trabajo del Mac (que deja el repo en *detached HEAD* después de cada build) ya se había visto en la Entrega 1 y volvió a aparecer aquí — recordatorio de que es una fragilidad de la infraestructura local, no de este entregable en particular.

Lo genuinamente nuevo en esta entrega fue trabajar con **datos en movimiento** en vez de solo en reposo: diseñar una fecha de corte que separe entrenamiento de "producción simulada", mantener la disciplina de que ninguna feature del modelo de riesgo mire datos posteriores a esa fecha, y aceptar honestamente las limitaciones de un modelo con fuerte desbalance de clases (eligiendo un umbral de alerta explícito en vez de ocultar un recall de cero).
