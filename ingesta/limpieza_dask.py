"""
Limpieza con Dask de negocios y reseñas filtrados (data/filtrado/*.jsonl),
construccion de la "vida de cada negocio" (dias-negocio, evento, censura,
entrada tardia) y carga a MongoDB como GeoJSON con indice 2dsphere.

Fuente:  data/filtrado/negocios.jsonl, data/filtrado/reviews.jsonl
Salida:  MongoDB -> negocios, reviews
"""
import os
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")

FILTRADO = Path("data/filtrado")
MIN_RESENAS_DIAS_NEGOCIO = 1  # evita negocios con 0 dias-negocio


def cargar_negocios_crudos():
    ddf = dd.read_json(str(FILTRADO / "negocios.jsonl"), lines=True)
    df = ddf.compute()

    columnas_numericas = ["latitude", "longitude", "stars", "review_count", "is_open"]
    for c in columnas_numericas:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["latitude", "longitude", "business_id"])
    df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]
    return df


def cargar_reviews_crudas():
    ddf = dd.read_json(str(FILTRADO / "reviews.jsonl"), lines=True)
    df = ddf.compute()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "business_id", "text"])
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce")

    # Etiqueta de sentimiento a partir de las estrellas (seccion 4 del enunciado)
    def etiqueta(estrellas):
        if estrellas <= 2:
            return "negativo"
        if estrellas == 3:
            return "neutral"
        return "positivo"

    df["sentimiento_estrellas"] = df["stars"].apply(etiqueta)
    return df


def construir_vida_negocios(negocios, reviews):
    agregados = (
        reviews.groupby("business_id")["date"]
        .agg(primera_resena="min", ultima_resena="max", n_resenas="count")
        .reset_index()
    )

    negocios = negocios.merge(agregados, on="business_id", how="inner")

    negocios["dias_negocio"] = (
        (negocios["ultima_resena"] - negocios["primera_resena"]).dt.days
    ).clip(lower=MIN_RESENAS_DIAS_NEGOCIO)

    # Evento: is_open = 0 -> cierre observado. is_open = 1 -> censurado.
    negocios["evento"] = (negocios["is_open"] == 0).astype(int)
    negocios["censurado"] = (negocios["is_open"] == 1).astype(int)

    # Entrada tardia (aproximacion, documentada en el informe): el dataset no
    # trae fecha de apertura, asi que se asume entrada tardia cuando la primera
    # resena del negocio cae dentro de los primeros 90 dias con datos del area
    # (no se puede descartar que el negocio ya existiera antes de esa ventana).
    inicio_ventana = reviews["date"].min()
    negocios["entrada_tardia"] = (
        (negocios["primera_resena"] - inicio_ventana).dt.days <= 90
    ).astype(int)

    return negocios


def a_geojson(negocios):
    negocios["location"] = negocios.apply(
        lambda r: {"type": "Point", "coordinates": [float(r["longitude"]), float(r["latitude"])]},
        axis=1,
    )
    return negocios


def main():
    print("Cargando negocios y reseñas filtrados con Dask...")
    negocios = cargar_negocios_crudos()
    reviews = cargar_reviews_crudas()
    print(f"  Negocios: {len(negocios):,}  |  Reseñas: {len(reviews):,}")

    negocios = construir_vida_negocios(negocios, reviews)
    negocios = a_geojson(negocios)

    negocios = negocios.where(pd.notnull(negocios), None)
    reviews_out = reviews.where(pd.notnull(reviews), None)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    db["negocios"].drop()
    db["reviews"].drop()

    db["negocios"].insert_many(negocios.to_dict("records"))
    db["reviews"].insert_many(reviews_out.to_dict("records"))

    db["negocios"].create_index([("location", "2dsphere")])
    db["negocios"].create_index("categories")
    db["reviews"].create_index("business_id")
    db["reviews"].create_index("date")

    print(f"Cargados {db['negocios'].count_documents({}):,} negocios y "
          f"{db['reviews'].count_documents({}):,} reseñas en MongoDB ({MONGO_DB}).")
    print(f"Cierres observados: {negocios['evento'].sum():,} / "
          f"Censurados (abiertos): {negocios['censurado'].sum():,} / "
          f"Entrada tardia (aprox.): {negocios['entrada_tardia'].sum():,}")


if __name__ == "__main__":
    main()
