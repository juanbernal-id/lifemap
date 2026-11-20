"""
E2.6 - Modelo de riesgo de cierre.

Patron de KafkaMed aplicado a negocios: en vez de riesgo de falla cardiaca,
riesgo de cierre de un negocio. Variables calculadas SOLO con datos hasta la
fecha de corte T (sin mirar nada posterior, para no filtrar informacion del
futuro al modelo):

  - sentimiento_medio_hasta_T
  - tendencia_pct_negativo_6m: % negativo en los 6 meses antes de T menos
    % negativo en los 6 meses anteriores a esos (positivo = empeorando)
  - estrellas (snapshot de business.json)
  - num_resenas_hasta_T
  - edad_dias_hasta_T: dias desde la primera resena hasta T
  - densidad_competidores_500m: negocios de la misma categoria principal a
    <=500m, consultados con $geoNear

Etiqueta (seccion "Modelo de riesgo de cierre" del enunciado):
  1  si is_open=0 y su ultima resena (en todo el dataset) cae en (T, T+12m]
  0  si is_open=1 (censurado) o si tiene actividad (resena) despues de T+12m
  excluido si no tuvo ninguna resena despues de T (no se puede saber que paso)
  excluido tambien si no tiene NINGUNA resena hasta T (no hay features)

Particion train/test POR NEGOCIO (mismo patron de E1.6, evita fuga).

Fuente: MongoDB -> negocios, reviews
Salida:
  - outputs/modelo_riesgo.pkl               (pipeline sklearn: features -> prob. de cierre)
  - outputs/metricas_riesgo.json            (AUC-ROC, AUC-PR, recall, matriz de confusion)
  - MongoDB -> negocios actualizado con el snapshot de features usado para entrenar
    (para que las alertas en streaming, E2.7, solo tengan que actualizar las partes
    que cambian con cada resena: sentimiento y tendencia)
"""
import json
import os
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")
FECHA_CORTE_T = datetime.fromisoformat(os.environ.get("FECHA_CORTE_T", "2021-01-01T00:00:00+00:00"))
VENTANA_ETIQUETA_MESES = 12
RADIO_COMPETIDORES_M = 500
UMBRAL_ALERTA = float(os.environ.get("UMBRAL_ALERTA_RIESGO", "0.5"))
SALIDA = Path(os.environ.get("SALIDA_DIR", "/app/outputs"))

PUNTAJE_SENTIMIENTO = {"positivo": 1, "neutral": 0, "negativo": -1}


def categoria_principal(categorias):
    if not categorias:
        return "Sin categoria"
    return categorias.split(",")[0].strip()


def t_mas_12_meses(t):
    # 365 dias es suficiente aproximacion de "12 meses" para este dataset
    return t + timedelta(days=365)


def cargar_negocios(db):
    cols = {
        "_id": 0, "business_id": 1, "name": 1, "categories": 1, "stars": 1,
        "is_open": 1, "location": 1, "primera_resena": 1, "ultima_resena": 1,
    }
    df = pd.DataFrame(list(db["negocios"].find({}, cols)))
    df["categoria_principal"] = df["categories"].apply(categoria_principal)
    for c in ("primera_resena", "ultima_resena"):
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    return df


def cargar_resenas_hasta_t(db, t):
    cols = {"_id": 0, "business_id": 1, "date": 1, "sentimiento_estrellas": 1}
    cursor = db["reviews"].find({"date": {"$lte": t}}, cols)
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df["puntaje"] = df["sentimiento_estrellas"].map(PUNTAJE_SENTIMIENTO).fillna(0)
    df["negativo"] = (df["sentimiento_estrellas"] == "negativo").astype(int)
    return df


def features_resenas(resenas, t):
    """sentimiento_medio_hasta_T, num_resenas_hasta_T, tendencia_pct_negativo_6m."""
    if resenas.empty:
        return pd.DataFrame(columns=["business_id", "sentimiento_medio_hasta_t",
                                      "num_resenas_hasta_t", "tendencia_pct_negativo_6m"])

    agg = resenas.groupby("business_id").agg(
        sentimiento_medio_hasta_t=("puntaje", "mean"),
        num_resenas_hasta_t=("puntaje", "count"),
    ).reset_index()

    ini_6m = t - timedelta(days=180)
    ini_12m = t - timedelta(days=360)

    ventana_reciente = resenas[(resenas["date"] > ini_6m) & (resenas["date"] <= t)]
    ventana_previa = resenas[(resenas["date"] > ini_12m) & (resenas["date"] <= ini_6m)]

    def pct_negativo(df):
        g = df.groupby("business_id")["negativo"].mean() * 100
        return g

    pct_reciente = pct_negativo(ventana_reciente).rename("pct_neg_reciente")
    pct_previo = pct_negativo(ventana_previa).rename("pct_neg_previo")

    tendencia = pd.concat([pct_reciente, pct_previo], axis=1).fillna(0)
    tendencia["tendencia_pct_negativo_6m"] = tendencia["pct_neg_reciente"] - tendencia["pct_neg_previo"]
    tendencia = tendencia.reset_index()[["business_id", "tendencia_pct_negativo_6m"]]

    return agg.merge(tendencia, on="business_id", how="left").fillna({"tendencia_pct_negativo_6m": 0})


def densidad_competidores(db, negocios_df):
    """Para cada negocio, cuantos negocios de su misma categoria principal hay
    a <= RADIO_COMPETIDORES_M metros, usando $geoNear (geoespacial, indice 2dsphere)."""
    densidades = {}
    for _, fila in negocios_df.iterrows():
        loc = fila.get("location")
        if not isinstance(loc, dict) or "coordinates" not in loc:
            densidades[fila["business_id"]] = 0
            continue
        pipeline = [
            {"$geoNear": {
                "near": loc,
                "distanceField": "distancia_m",
                "maxDistance": RADIO_COMPETIDORES_M,
                "spherical": True,
                "key": "location",
            }},
            {"$match": {"business_id": {"$ne": fila["business_id"]}}},
            {"$count": "n"},
        ]
        resultado = list(db["negocios"].aggregate(pipeline))
        densidades[fila["business_id"]] = resultado[0]["n"] if resultado else 0
    return pd.Series(densidades, name="densidad_competidores_500m")


def construir_etiqueta(negocios_df, t):
    t12 = t_mas_12_meses(t)
    tiene_actividad_despues_t = negocios_df["ultima_resena"] > t
    sigue_abierto = negocios_df["is_open"] == 1
    actividad_despues_t12 = negocios_df["ultima_resena"] > t12
    cierra_en_ventana = (~sigue_abierto) & (negocios_df["ultima_resena"] <= t12) & tiene_actividad_despues_t

    etiqueta = pd.Series(np.nan, index=negocios_df.index)
    etiqueta[tiene_actividad_despues_t & sigue_abierto] = 0
    etiqueta[tiene_actividad_despues_t & (~sigue_abierto) & actividad_despues_t12] = 0
    etiqueta[tiene_actividad_despues_t & cierra_en_ventana] = 1
    # sin actividad despues de T -> se queda en NaN -> se excluye
    return etiqueta


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print(f"Fecha de corte T = {FECHA_CORTE_T.isoformat()}")
    negocios = cargar_negocios(db)
    print(f"Negocios totales: {len(negocios):,}")

    resenas_t = cargar_resenas_hasta_t(db, FECHA_CORTE_T)
    print(f"Reseñas hasta T: {len(resenas_t):,} (de {db['reviews'].count_documents({}):,} totales)")
    feats_resenas = features_resenas(resenas_t, FECHA_CORTE_T)

    negocios = negocios.merge(feats_resenas, on="business_id", how="left")
    # Sin ninguna reseña hasta T -> sin historia -> se excluye de entrenamiento
    tiene_historia = negocios["num_resenas_hasta_t"].notna() & (negocios["num_resenas_hasta_t"] > 0)

    negocios["edad_dias_hasta_t"] = (FECHA_CORTE_T - negocios["primera_resena"]).dt.days.clip(lower=0)
    negocios["etiqueta"] = construir_etiqueta(negocios, FECHA_CORTE_T)

    entrenables = negocios[tiene_historia & negocios["etiqueta"].notna()].copy()
    print(f"Negocios con historia hasta T y etiqueta definida: {len(entrenables):,} "
          f"(excluidos por falta de historia o de actividad posterior a T: "
          f"{len(negocios) - len(entrenables):,})")

    print("Calculando densidad de competidores a 500m con $geoNear (puede tardar unos minutos)...")
    densidad = densidad_competidores(db, entrenables)
    entrenables = entrenables.merge(densidad.rename("densidad_competidores_500m"), left_on="business_id",
                                     right_index=True, how="left")
    entrenables["densidad_competidores_500m"] = entrenables["densidad_competidores_500m"].fillna(0)

    columnas_features = [
        "sentimiento_medio_hasta_t", "tendencia_pct_negativo_6m", "stars",
        "num_resenas_hasta_t", "edad_dias_hasta_t", "densidad_competidores_500m",
    ]
    X = entrenables[columnas_features].fillna(0)
    y = entrenables["etiqueta"].astype(int)

    print(f"\nDistribucion de la etiqueta (desbalanceada, se interpreta con AUC-PR):")
    print(y.value_counts())

    # Particion por negocio (no hay repetidos aqui, una fila = un negocio, pero
    # se usa el mismo hash por business_id que en E1.6 para ser consistentes)
    import hashlib
    fold = entrenables["business_id"].apply(
        lambda b: int(hashlib.md5(b.encode()).hexdigest(), 16) % 10
    )
    train_idx = fold < 8
    X_train, X_test = X[train_idx], X[~train_idx]
    y_train, y_test = y[train_idx], y[~train_idx]
    print(f"Train: {len(X_train):,} negocios | Test: {len(X_test):,} negocios")

    modelo = RandomForestClassifier(
        n_estimators=300, max_depth=8, min_samples_leaf=5,
        class_weight="balanced", random_state=42, n_jobs=-1,
    )
    modelo.fit(X_train, y_train)

    proba_test = modelo.predict_proba(X_test)[:, 1]
    pred_test = (proba_test >= UMBRAL_ALERTA).astype(int)

    auc_roc = roc_auc_score(y_test, proba_test)
    auc_pr = average_precision_score(y_test, proba_test)
    recall = recall_score(y_test, pred_test)
    matriz = confusion_matrix(y_test, pred_test).tolist()
    tasa_positivos = float(y.mean())

    print(f"\nAUC-ROC: {auc_roc:.4f}")
    print(f"AUC-PR:  {auc_pr:.4f}  (linea base = tasa de positivos = {tasa_positivos:.4f}; "
          f"el desbalance hace que AUC-PR sea la metrica mas informativa que AUC-ROC)")
    print(f"Recall en umbral {UMBRAL_ALERTA}: {recall:.4f}")
    print(f"Matriz de confusion (filas=real, cols=prediccion) [[TN,FP],[FN,TP]]: {matriz}")
    importancias = dict(zip(columnas_features, modelo.feature_importances_.round(4).tolist()))
    print(f"Importancia de variables: {importancias}")

    SALIDA.mkdir(parents=True, exist_ok=True)
    with open(SALIDA / "modelo_riesgo.pkl", "wb") as f:
        pickle.dump({"modelo": modelo, "columnas": columnas_features, "umbral": UMBRAL_ALERTA}, f)

    metricas = {
        "fecha_corte_t": FECHA_CORTE_T.isoformat(),
        "umbral": UMBRAL_ALERTA,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "tasa_positivos": round(tasa_positivos, 4),
        "auc_roc": round(float(auc_roc), 4),
        "auc_pr": round(float(auc_pr), 4),
        "recall": round(float(recall), 4),
        "matriz_confusion": matriz,
        "importancia_variables": importancias,
    }
    with open(SALIDA / "metricas_riesgo.json", "w") as f:
        json.dump(metricas, f, ensure_ascii=False, indent=2)
    print(f"\nModelo guardado en {SALIDA/'modelo_riesgo.pkl'}; metricas en {SALIDA/'metricas_riesgo.json'}")

    # Snapshot de features en Mongo para que las alertas en streaming (E2.7)
    # solo tengan que actualizar lo que cambia con cada reseña nueva.
    actualizaciones = entrenables[["business_id"] + columnas_features].to_dict("records")
    for reg in actualizaciones:
        bid = reg.pop("business_id")
        db["negocios"].update_one({"business_id": bid}, {"$set": {"riesgo_features_t": reg}})
    print(f"Snapshot de features (hasta T) guardado en negocios.riesgo_features_t "
          f"para {len(actualizaciones):,} negocios.")


if __name__ == "__main__":
    main()
