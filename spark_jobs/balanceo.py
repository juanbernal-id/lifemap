"""
E1.8 - Balanceo de carga.

Mismo experimento del ejercicio de discos: tiempos de MapReduce con
2/8/32/200 particiones (3 repeticiones c/u) y comparacion de reparto
CON clave (por categoria/ciudad) vs SIN clave, usando spark_partition_id().

Fuente: MongoDB -> negocios
Salida: imprime la tabla de tiempos y el conteo de filas por particion
(se guarda tambien en outputs/benchmarks.json para el informe).
"""
import json
import time
from pathlib import Path

from pyspark.sql import functions as F

from comun import crear_sesion

REPETICIONES = 3
PARTICIONES = [2, 8, 32, 200]


def correr_mapreduce(df_particionado):
    rdd = df_particionado.select("categoria_principal", "dias_negocio", "evento").rdd
    pares = rdd.map(lambda f: (f["categoria_principal"], (int(f["dias_negocio"] or 0), int(f["evento"] or 0))))
    agregados = pares.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return agregados.collect()


def medir_tiempo(df, n_particiones, repeticiones=REPETICIONES):
    tiempos = []
    for i in range(repeticiones):
        df_rep = df.repartition(n_particiones)
        inicio = time.time()
        correr_mapreduce(df_rep)
        tiempos.append(time.time() - inicio)
        print(f"  Particiones={n_particiones} | repeticion {i+1}/{repeticiones} | {round(tiempos[-1], 2)}s")
    return tiempos


def main():
    spark = crear_sesion("lifemap-balanceo", coleccion_lectura="negocios")

    categoria_udf = F.udf(lambda c: (c.split(",")[0].strip() if c else "Sin categoria"))
    df = spark.read.format("mongodb").load()
    df = df.withColumn("categoria_principal", categoria_udf(F.col("categories")))
    df.cache()
    df.count()

    resultados = {}
    print("\n=== Tabla de tiempos por numero de particiones ===")
    for n in PARTICIONES:
        print(f"--- {n} particiones ---")
        tiempos = medir_tiempo(df, n)
        resultados[n] = {
            "tiempos": tiempos,
            "promedio": sum(tiempos) / len(tiempos),
            "desviacion": (sum((t - sum(tiempos) / len(tiempos)) ** 2 for t in tiempos) / len(tiempos)) ** 0.5,
        }
        print(f"  Promedio: {round(resultados[n]['promedio'], 3)}s | "
              f"Desv.: {round(resultados[n]['desviacion'], 3)}s\n")

    print("\n=== Sesgo: reparto CON clave (categoria) vs SIN clave ===")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

    df_con_clave = df.repartition(200, "categoria_principal")
    conteo_con_clave = (
        df_con_clave.withColumn("particion", F.spark_partition_id())
        .groupBy("particion").count().orderBy(F.desc("count"))
    )
    print("Reparto CON clave (por categoria):")
    conteo_con_clave.show(10)
    filas_con_clave = [r["count"] for r in conteo_con_clave.collect()]

    df_sin_clave = df.repartition(200)
    conteo_sin_clave = (
        df_sin_clave.withColumn("particion", F.spark_partition_id())
        .groupBy("particion").count().orderBy(F.desc("count"))
    )
    print("Reparto SIN clave (uniforme):")
    conteo_sin_clave.show(10)
    filas_sin_clave = [r["count"] for r in conteo_sin_clave.collect()]

    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    sesgo = {
        "con_clave": {"max": max(filas_con_clave), "min": min(filas_con_clave)},
        "sin_clave": {"max": max(filas_sin_clave), "min": min(filas_sin_clave)},
    }
    print(f"\nSesgo CON clave: max={sesgo['con_clave']['max']} min={sesgo['con_clave']['min']}")
    print(f"Sesgo SIN clave: max={sesgo['sin_clave']['max']} min={sesgo['sin_clave']['min']}")

    Path("/app/outputs").mkdir(parents=True, exist_ok=True)
    with open("/app/outputs/benchmarks.json", "w") as f:
        json.dump({"tiempos_por_particion": resultados, "sesgo": sesgo}, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
