"""Utilidades compartidas por los jobs de Spark de LifeMap."""
import os
import subprocess

from pyspark.sql import SparkSession

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
CONECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.7.0"


def _ip_propia():
    return subprocess.check_output("hostname -I | awk '{print $1}'", shell=True, text=True).strip()


def crear_sesion(nombre, coleccion_lectura="negocios", memoria_executor="1g", cores_max="2"):
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(nombre)
        .config("spark.jars.packages", CONECTOR)
        .config("spark.driver.host", _ip_propia())
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .config("spark.executor.memory", memoria_executor)
        .config("spark.cores.max", cores_max)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", MONGO_DB)
        .config("spark.mongodb.read.collection", coleccion_lectura)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", MONGO_DB)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
