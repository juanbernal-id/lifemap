"""
E1.6 - Modelo de sentimiento.

No existe "SentimentStream" para este equipo, asi que se entrena un
pipeline minimo pero real con Spark MLlib: limpieza -> tokenizacion ->
stopwords -> HashingTF/IDF -> Naive Bayes y regresion logistica, comparados
con precision/recall/F1 por clase y matriz de confusion.

La particion entrenamiento/prueba es POR NEGOCIO (no por reseña), para que
un mismo negocio no quede en los dos lados (evita fuga, tema 2 de la
diapositiva del profe).

Fuente: MongoDB -> reviews (texto + sentimiento_estrellas)
Salida:
  - /app/outputs/modelo_sentimiento        (PipelineModel de Spark, el que
    se evalua y el que reutiliza la Entrega 2 sin reentrenar)
  - /app/outputs/modelo_sentimiento_sklearn.pkl  (TF-IDF + LogisticRegression
    de scikit-learn, entrenado sobre los mismos datos, SOLO para que la API
    Flask (sin Spark) pueda servir /predict sin levantar una SparkSession
    por request)
"""
import pickle
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, StringIndexer, Tokenizer
from pyspark.sql import functions as F

from comun import crear_sesion


def exportar_modelo_servible(train_pd):
    """Entrena un modelo liviano equivalente (sklearn) solo para /predict en Flask."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression as SkLogReg
    from sklearn.pipeline import Pipeline as SkPipeline

    pipeline = SkPipeline([
        ("tfidf", TfidfVectorizer(max_features=20000, stop_words="english")),
        ("clf", SkLogReg(max_iter=200)),
    ])
    pipeline.fit(train_pd["text"], train_pd["sentimiento_estrellas"])

    Path("/app/outputs").mkdir(parents=True, exist_ok=True)
    with open("/app/outputs/modelo_sentimiento_sklearn.pkl", "wb") as f:
        pickle.dump(pipeline, f)
    print("Modelo servible (sklearn) guardado en /app/outputs/modelo_sentimiento_sklearn.pkl")


def main():
    spark = crear_sesion("lifemap-sentimiento", coleccion_lectura="reviews")
    df = spark.read.format("mongodb").load().select("business_id", "text", "sentimiento_estrellas")
    df = df.na.drop(subset=["text", "sentimiento_estrellas"])

    print(f"Total de reseñas: {df.count():,}")
    print("Distribucion de clases (desbalanceada, se reporta como pide el enunciado):")
    df.groupBy("sentimiento_estrellas").count().orderBy(F.desc("count")).show()

    # Particion por negocio: cada business_id cae entero en train o test.
    negocios = df.select("business_id").distinct()
    negocios = negocios.withColumn("fold", F.abs(F.hash("business_id")) % 10)
    train_ids = negocios.filter("fold < 8").select("business_id")
    test_ids = negocios.filter("fold >= 8").select("business_id")

    train = df.join(train_ids, "business_id")
    test = df.join(test_ids, "business_id")
    print(f"Train: {train.count():,} reseñas | Test: {test.count():,} reseñas")

    indexador = StringIndexer(inputCol="sentimiento_estrellas", outputCol="label")
    tokenizer = Tokenizer(inputCol="text", outputCol="palabras")
    stopwords = StopWordsRemover(inputCol="palabras", outputCol="palabras_limpias")
    hashing_tf = HashingTF(inputCol="palabras_limpias", outputCol="tf", numFeatures=20000)
    idf = IDF(inputCol="tf", outputCol="features")

    evaluador = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    modelos = {
        "naive_bayes": NaiveBayes(labelCol="label", featuresCol="features"),
        "regresion_logistica": LogisticRegression(labelCol="label", featuresCol="features", maxIter=20),
    }

    mejor_modelo = None
    mejor_f1 = -1
    mejor_nombre = None

    for nombre, clasificador in modelos.items():
        pipeline = Pipeline(stages=[indexador, tokenizer, stopwords, hashing_tf, idf, clasificador])
        modelo = pipeline.fit(train)
        predicciones = modelo.transform(test)

        f1 = evaluador.evaluate(predicciones, {evaluador.metricName: "f1"})
        precision = evaluador.evaluate(predicciones, {evaluador.metricName: "weightedPrecision"})
        recall = evaluador.evaluate(predicciones, {evaluador.metricName: "weightedRecall"})

        print(f"\n=== {nombre} ===")
        print(f"F1 (macro ponderado): {f1:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f}")
        print("Matriz de confusion (label real vs prediccion):")
        predicciones.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

        if f1 > mejor_f1:
            mejor_f1 = f1
            mejor_modelo = modelo
            mejor_nombre = nombre

    print(f"\nMejor modelo: {mejor_nombre} (F1={mejor_f1:.4f})")
    mejor_modelo.write().overwrite().save("/app/outputs/modelo_sentimiento")
    print("Modelo guardado en /app/outputs/modelo_sentimiento")

    # Modelo servible para la API Flask (sin Spark)
    muestra = train.select("text", "sentimiento_estrellas").limit(200_000).toPandas()
    exportar_modelo_servible(muestra)

    spark.stop()


if __name__ == "__main__":
    main()
