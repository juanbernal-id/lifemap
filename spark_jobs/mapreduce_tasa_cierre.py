"""
E1.7 - MapReduce y tasa de cierre.

Mismo patron del ejercicio de discos ("cuanto viven los discos"): en vez de
(modelo, (dias_disco, falla)) usamos (categoria/celda, (dias_negocio, cierre)).

Fuente: MongoDB -> negocios (con dias_negocio, evento, location)
Salida: MongoDB -> tasas_cierre (una fila por categoria principal y por celda geohash)

Se valida el resultado de reduceByKey contra un groupBy de DataFrames, como
pide el criterio E1.7.
"""
from pyspark.sql import functions as F

from comun import crear_sesion

GEOHASH_PRECISION = 4  # ~20km de lado, suficiente para una sola area metropolitana


def categoria_principal(categorias):
    if not categorias:
        return "Sin categoria"
    return categorias.split(",")[0].strip()


def geohash_simple(lat, lon, precision=GEOHASH_PRECISION):
    # Geohash minimo (sin libreria externa): redondeo a una grilla fija,
    # suficiente para agrupar negocios cercanos en celdas.
    escala = 10 ** (precision - 2)
    return f"{round(lat * escala) / escala}_{round(lon * escala) / escala}"


def main():
    spark = crear_sesion("lifemap-mapreduce-tasa-cierre", coleccion_lectura="negocios")

    categoria_udf = F.udf(categoria_principal)
    geohash_udf = F.udf(geohash_simple)

    df = spark.read.format("mongodb").load()
    df = df.withColumn("categoria_principal", categoria_udf(F.col("categories")))
    df = df.withColumn(
        "geohash",
        geohash_udf(F.col("location.coordinates")[1], F.col("location.coordinates")[0]),
    )

    for clave in ["categoria_principal", "geohash"]:
        print(f"\n=== Tasa de cierre por {clave} (MapReduce / RDD) ===")

        rdd = df.select(clave, "dias_negocio", "evento").rdd
        pares = rdd.map(lambda f: (f[clave], (int(f["dias_negocio"] or 0), int(f["evento"] or 0))))
        agregados = pares.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

        def calcular_tasa(registro):
            k, (dias_negocio, cierres) = registro
            tasa = (cierres / (dias_negocio / 365)) * 100 if dias_negocio > 0 else None
            return (k, dias_negocio, cierres, tasa)

        resultado_rdd = agregados.map(calcular_tasa)
        cols = [clave, "dias_negocio", "cierres", "tasa_cierre_pct"]
        df_resultado = spark.createDataFrame(resultado_rdd, cols).orderBy(
            F.desc("tasa_cierre_pct")
        )
        df_resultado.show(20, truncate=False)

        # Validacion contra groupBy de DataFrames (pide el criterio E1.7)
        df_groupby = (
            df.groupBy(clave)
            .agg(F.sum("dias_negocio").alias("dias_negocio_gb"), F.sum("evento").alias("cierres_gb"))
        )
        comparacion = df_resultado.join(
            df_groupby, df_resultado[clave] == df_groupby[clave]
        ).select(
            df_resultado[clave],
            "dias_negocio", "dias_negocio_gb",
            "cierres", "cierres_gb",
        )
        no_coincide = comparacion.filter(
            (F.col("dias_negocio") != F.col("dias_negocio_gb"))
            | (F.col("cierres") != F.col("cierres_gb"))
        ).count()
        print(f"Filas que NO coinciden entre reduceByKey y groupBy: {no_coincide}")
        assert no_coincide == 0, "El resultado de reduceByKey no coincide con el groupBy"

        (
            df_resultado.write.format("mongodb")
            .option("collection", f"tasas_cierre_{clave}")
            .mode("overwrite")
            .save()
        )

    spark.stop()


if __name__ == "__main__":
    main()
