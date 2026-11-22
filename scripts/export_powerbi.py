#!/usr/bin/env python3
"""
LifeMap - exporta un snapshot limpio de Mongo a un .xlsx de 4 hojas,
listo para subir a app.powerbi.com (Power BI Service, sin instalar nada).

Hojas:
  1. Negocios   - business_id, nombre, categoria, lat, lon, probabilidad_cierre,
                  sentimiento_actualizado, resenas_desde_t  (mismo calculo que /risk/near)
  2. Alertas    - alertas activas con nombre, categoria, probabilidad, fecha
  3. Supervivencia - puntos de la curva Kaplan-Meier por categoria ("todos")
  4. Evolucion  - serie temporal de % de reseñas negativas (geohash agregado)

Se corre DENTRO de la red de docker-compose (lifemap_network) para poder
resolver el host "mongo", con pymongo + openpyxl instalados al vuelo.
"""
import pickle
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "lifemap"
MODELO_PATH = Path("/app/outputs/modelo_riesgo.pkl")
SALIDA = Path("/app/demo/lifemap_powerbi.xlsx")

ENCABEZADO_FILL = PatternFill(start_color="1F6F54", end_color="1F6F54", fill_type="solid")
ENCABEZADO_FONT = Font(color="FFFFFF", bold=True)


def escribir_hoja(wb, nombre, columnas, filas):
    ws = wb.create_sheet(nombre)
    ws.append(columnas)
    for col_idx in range(1, len(columnas) + 1):
        celda = ws.cell(row=1, column=col_idx)
        celda.font = ENCABEZADO_FONT
        celda.fill = ENCABEZADO_FILL
    for fila in filas:
        ws.append(fila)
    for col_idx, nombre_col in enumerate(columnas, start=1):
        ancho = max(12, min(40, len(str(nombre_col)) + 4))
        ws.column_dimensions[get_column_letter(col_idx)].width = ancho
    ws.freeze_panes = "A2"
    return ws


def main():
    cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db = cliente[MONGO_DB]

    if not MODELO_PATH.exists():
        raise SystemExit(
            f"No encontre el modelo entrenado en {MODELO_PATH}. "
            "Corre spark_jobs/riesgo_cierre.py primero."
        )
    with open(MODELO_PATH, "rb") as f:
        modelo_riesgo = pickle.load(f)
    modelo = modelo_riesgo["modelo"]

    wb = Workbook()
    wb.remove(wb.active)  # quitamos la hoja vacia por defecto

    # --- 1. Negocios, con la misma puntuacion de riesgo que usa /risk/near ---
    filas_negocios = []
    cursor = db["negocios"].find({"riesgo_features_t": {"$exists": True}})
    for d in cursor:
        ft = d["riesgo_features_t"]
        num_stream = d.get("num_resenas_stream", 0)
        suma_stream = d.get("suma_puntajes_stream", 0.0)
        num_total = ft["num_resenas_hasta_t"] + num_stream
        suma_total = ft["sentimiento_medio_hasta_t"] * ft["num_resenas_hasta_t"] + suma_stream
        sentimiento_actualizado = suma_total / num_total if num_total else 0.0

        fila_modelo = [[
            sentimiento_actualizado,
            ft["tendencia_pct_negativo_6m"],
            ft["stars"],
            num_total,
            ft["edad_dias_hasta_t"],
            ft["densidad_competidores_500m"],
        ]]
        probabilidad = float(modelo.predict_proba(fila_modelo)[0][1])
        coords = d.get("location", {}).get("coordinates")
        if not coords:
            continue

        filas_negocios.append([
            d["business_id"],
            d.get("name"),
            d.get("categories", "").split(",")[0].strip() if d.get("categories") else None,
            coords[1],  # lat
            coords[0],  # lon
            round(probabilidad, 4),
            round(sentimiento_actualizado, 4),
            int(num_total),
            bool(d.get("is_open", 1)),
        ])
    filas_negocios.sort(key=lambda r: r[5], reverse=True)
    escribir_hoja(
        wb, "Negocios",
        ["business_id", "nombre", "categoria", "lat", "lon",
         "probabilidad_cierre", "sentimiento_actualizado", "resenas_desde_t", "abierto"],
        filas_negocios,
    )
    print(f"Negocios: {len(filas_negocios)} filas")

    # --- 2. Alertas activas, con nombre del negocio ---
    filas_alertas = []
    alertas = list(db["alerts"].find({}).sort("probabilidad_cierre", -1))
    nombres = {
        n["business_id"]: n.get("name")
        for n in db["negocios"].find(
            {"business_id": {"$in": [a["business_id"] for a in alertas]}},
            {"business_id": 1, "name": 1, "_id": 0},
        )
    }
    for a in alertas:
        filas_alertas.append([
            nombres.get(a["business_id"], a["business_id"]),
            a.get("business_id"),
            a.get("categories", "").split(",")[0].strip() if a.get("categories") else None,
            round(a.get("probabilidad_cierre", 0.0), 4),
            round(a.get("sentimiento_medio_actualizado", 0.0), 4) if a.get("sentimiento_medio_actualizado") is not None else None,
            a.get("fecha"),
        ])
    escribir_hoja(
        wb, "Alertas",
        ["nombre", "business_id", "categoria", "probabilidad_cierre", "sentimiento_actualizado", "fecha"],
        filas_alertas,
    )
    print(f"Alertas: {len(filas_alertas)} filas")

    # --- 3. Supervivencia (curva Kaplan-Meier "todos", por categoria) ---
    filas_superv = []
    for doc in db["supervivencia_categoria"].find({}):
        cat = doc.get("categoria_principal")
        todos = doc.get("todos")
        if not todos:
            continue
        for punto in todos.get("puntos", []):
            filas_superv.append([
                cat,
                todos.get("n"),
                todos.get("mediana_dias"),
                punto.get("dias"),
                punto.get("supervivencia"),
                punto.get("en_riesgo"),
            ])
    escribir_hoja(
        wb, "Supervivencia",
        ["categoria", "n_negocios", "mediana_dias", "dias", "supervivencia", "en_riesgo"],
        filas_superv,
    )
    print(f"Supervivencia: {len(filas_superv)} filas")

    # --- 4. Evolucion temporal (% negativo agregado por ventana de 30 dias) ---
    filas_evol = []
    pipeline = [
        {"$group": {"_id": "$ventana_inicio",
                     "n_resenas": {"$sum": "$n_resenas"},
                     "n_negativas": {"$sum": "$n_negativas"},
                     "celdas": {"$sum": 1},
                     "hotspots": {"$sum": {"$cond": ["$hotspot", 1, 0]}}}},
        {"$sort": {"_id": 1}},
    ]
    for d in db["ventanas_geohash"].aggregate(pipeline):
        pct = round(d["n_negativas"] / d["n_resenas"] * 100, 2) if d["n_resenas"] else 0.0
        filas_evol.append([d["_id"], d["n_resenas"], d["n_negativas"], pct, d["celdas"], d["hotspots"]])
    escribir_hoja(
        wb, "Evolucion",
        ["ventana_inicio", "n_resenas", "n_negativas", "pct_negativo", "celdas", "hotspots"],
        filas_evol,
    )
    print(f"Evolucion: {len(filas_evol)} filas")

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    wb.save(SALIDA)
    print(f"\nListo: {SALIDA}  (generado {datetime.utcnow().isoformat()}Z)")
    cliente.close()


if __name__ == "__main__":
    main()
