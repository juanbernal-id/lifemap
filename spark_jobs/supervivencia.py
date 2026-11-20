"""
E2.5 - Supervivencia (Kaplan-Meier).

Curvas por categoria principal y por zona (celda de geohash, la misma grilla
de spark_jobs/mapreduce_tasa_cierre.py), consultando cada zona con $geoWithin
sobre un poligono rectangular que delimita la celda.

Censura: evento=0 (is_open=1) son negocios censurados por la derecha (solo se
sabe que seguian abiertos en la fecha de corte del dataset).

Entrada tardia: el dataset no trae fecha de apertura, asi que para un negocio
con entrada_tardia=1 (su primera resena cae en los primeros 90 dias con datos
del area, ver E1) no se puede saber cuanto tiempo llevaba operando antes de esa
primera resena -> su dias_negocio subestima su edad real, pero no hay forma de
corregir el sesgo sin inventar un numero. En vez de adivinar una entrada, se
reporta la curva dos veces por grupo: "todos" (con el sesgo conservador ya
documentado en el informe de Entrega 1) y "sin entrada tardia" (excluye esos
negocios -> curva libre de ese sesgo, es la que se usa para comparar grupos).

Minimo de negocios por grupo (para no sacar conclusiones de grupos pequeños,
mismo espiritu del minimo de dias-negocio de E1.7): MIN_NEGOCIOS_POR_GRUPO.

Fuente: MongoDB -> negocios
Salida: MongoDB -> supervivencia_categoria, supervivencia_zona
"""
import os
from pathlib import Path

import pandas as pd
from lifelines import KaplanMeierFitter
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
GEOHASH_PRECISION = 4
MIN_NEGOCIOS_POR_GRUPO = 30


def categoria_principal(categorias):
    if not categorias:
        return "Sin categoria"
    return categorias.split(",")[0].strip()


def geohash_simple(lat, lon, precision=GEOHASH_PRECISION):
    escala = 10 ** (precision - 2)
    return f"{round(lat * escala) / escala}_{round(lon * escala) / escala}"


def celda_a_poligono(celda, precision=GEOHASH_PRECISION):
    """Poligono rectangular GeoJSON que cubre la celda de geohash (centro +- medio paso)."""
    lat_str, lon_str = celda.split("_")
    lat, lon = float(lat_str), float(lon_str)
    paso = 1 / (10 ** (precision - 2))
    medio = paso / 2
    anillo = [
        [lon - medio, lat - medio], [lon + medio, lat - medio],
        [lon + medio, lat + medio], [lon - medio, lat + medio],
        [lon - medio, lat - medio],
    ]
    return {"type": "Polygon", "coordinates": [anillo]}


def curva_km(df):
    """Punto a punto de una curva KM: timeline, supervivencia, en_riesgo."""
    kmf = KaplanMeierFitter()
    kmf.fit(durations=df["dias_negocio"], event_observed=df["evento"])
    tabla = kmf.survival_function_.reset_index()
    tabla.columns = ["dias", "supervivencia"]
    en_riesgo = kmf.event_table["at_risk"].reset_index()
    en_riesgo.columns = ["dias", "en_riesgo"]
    tabla = tabla.merge(en_riesgo, on="dias", how="left")
    mediana = kmf.median_survival_time_
    puntos = tabla.iloc[:: max(1, len(tabla) // 60)].to_dict("records")  # ~60 puntos, suficiente para graficar
    return {
        "n": int(len(df)),
        "eventos": int(df["evento"].sum()),
        "mediana_dias": None if pd.isna(mediana) else round(float(mediana), 1),
        "puntos": [
            {"dias": round(float(p["dias"]), 1), "supervivencia": round(float(p["supervivencia"]), 4),
             "en_riesgo": int(p["en_riesgo"])}
            for p in puntos
        ],
    }


def resumen_grupo(df):
    resumen = {"todos": None, "sin_entrada_tardia": None}
    if len(df) >= MIN_NEGOCIOS_POR_GRUPO:
        resumen["todos"] = curva_km(df)
    sin_tardia = df[df["entrada_tardia"] == 0]
    if len(sin_tardia) >= MIN_NEGOCIOS_POR_GRUPO:
        resumen["sin_entrada_tardia"] = curva_km(sin_tardia)
    return resumen


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    cols = {"_id": 0, "business_id": 1, "categories": 1, "location": 1,
            "dias_negocio": 1, "evento": 1, "entrada_tardia": 1}
    negocios = pd.DataFrame(list(db["negocios"].find({}, cols)))
    negocios["categoria_principal"] = negocios["categories"].apply(categoria_principal)
    negocios["geohash"] = negocios.apply(
        lambda r: geohash_simple(r["location"]["coordinates"][1], r["location"]["coordinates"][0])
        if isinstance(r.get("location"), dict) else None,
        axis=1,
    )
    print(f"Negocios: {len(negocios):,}")

    print("\n=== Supervivencia por categoria principal ===")
    docs_categoria = []
    for categoria, grupo in negocios.groupby("categoria_principal"):
        resumen = resumen_grupo(grupo)
        if resumen["todos"] is None and resumen["sin_entrada_tardia"] is None:
            continue
        print(f"  {categoria}: n={len(grupo)}  mediana(todos)={resumen['todos']['mediana_dias'] if resumen['todos'] else 'n/a'}")
        docs_categoria.append({"categoria_principal": categoria, **resumen})
    db["supervivencia_categoria"].drop()
    if docs_categoria:
        db["supervivencia_categoria"].insert_many(docs_categoria)

    print("\n=== Supervivencia por zona (celda de geohash, consultada con $geoWithin) ===")
    docs_zona = []
    for celda in negocios["geohash"].dropna().unique():
        poligono = celda_a_poligono(celda)
        # Se consulta la zona con $geoWithin (requisito del enunciado), no se
        # reusa directamente el DataFrame en memoria para esta parte.
        ids_en_zona = [
            d["business_id"] for d in db["negocios"].find(
                {"location": {"$geoWithin": {"$geometry": poligono}}}, {"_id": 0, "business_id": 1}
            )
        ]
        grupo = negocios[negocios["business_id"].isin(ids_en_zona)]
        resumen = resumen_grupo(grupo)
        if resumen["todos"] is None and resumen["sin_entrada_tardia"] is None:
            continue
        docs_zona.append({"geohash": celda, "poligono": poligono, **resumen})
    print(f"  Zonas con al menos {MIN_NEGOCIOS_POR_GRUPO} negocios: {len(docs_zona)}")
    db["supervivencia_zona"].drop()
    if docs_zona:
        db["supervivencia_zona"].insert_many(docs_zona)

    print(f"\nGuardado: {len(docs_categoria)} curvas por categoria, {len(docs_zona)} curvas por zona.")


if __name__ == "__main__":
    main()
