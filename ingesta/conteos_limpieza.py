"""
Conteos antes/despues de cada regla de limpieza (criterio E1.4).

Lee con Dask, de forma particionada, los archivos filtrados del area
(data/filtrado/*.jsonl) y aplica las reglas de limpieza una por una,
reportando cuantos registros elimina cada una. Resultado en
outputs/conteos_limpieza.json.

Uso (dentro de la imagen de Dask, montando el repo):
    docker run --rm -v "$PWD":/app -w /app lifemap-dask python ingesta/conteos_limpieza.py
"""
import json
import os
from pathlib import Path

import dask.dataframe as dd
from dask.distributed import Client

FILTRADO = Path(os.environ.get("FILTRADO_DIR", "data/filtrado")).resolve()
DASK_SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")


def conectar_cluster():
    """Usa el scheduler del Compose (2 workers); si no esta, Dask local."""
    try:
        client = Client(DASK_SCHEDULER, timeout="10s")
        workers = len(client.scheduler_info()["workers"])
        print(f"Conectado al cluster Dask {DASK_SCHEDULER} ({workers} workers)")
        return client
    except Exception as e:
        print(f"Sin cluster Dask ({type(e).__name__}); se usa el planificador local")
        return None
BLOQUE = "64MB"  # lectura particionada


def aplicar(nombre, ddf, filtro, pasos, justificacion):
    antes = len(ddf)
    ddf = filtro(ddf)
    despues = len(ddf)
    pasos.append({"regla": nombre, "antes": antes, "despues": despues,
                  "eliminados": antes - despues, "justificacion": justificacion})
    print(f"  {nombre:<45} {antes:>9,} -> {despues:>9,}  (-{antes - despues:,})")
    return ddf


def main():
    client = conectar_cluster()
    resultado = {"cluster": DASK_SCHEDULER if client else "local"}

    print("Negocios (data/filtrado/negocios.jsonl):")
    neg = dd.read_json(str(FILTRADO / "negocios.jsonl"), lines=True, blocksize=BLOQUE, dtype=False, convert_dates=False)
    print(f"  particiones Dask: {neg.npartitions}")
    pasos = []
    neg = aplicar("coordenadas nulas", neg,
                  lambda d: d.dropna(subset=["latitude", "longitude", "business_id"]), pasos,
                  "sin coordenadas no se puede construir el GeoJSON ni consultar por ubicacion")
    neg = aplicar("coordenadas fuera de rango", neg,
                  lambda d: d[d["latitude"].astype(float).between(-90, 90) & d["longitude"].astype(float).between(-180, 180)], pasos,
                  "MongoDB rechaza puntos fuera de [-90,90]x[-180,180] en el indice 2dsphere")
    neg = aplicar("business_id duplicado", neg,
                  lambda d: d.drop_duplicates(subset=["business_id"]), pasos,
                  "un negocio duplicado contaria doble en cierres y dias-negocio")
    resultado["negocios"] = pasos

    print("Resenas (data/filtrado/reviews.jsonl):")
    rev = dd.read_json(str(FILTRADO / "reviews.jsonl"), lines=True, blocksize=BLOQUE, dtype=False, convert_dates=False)
    print(f"  particiones Dask: {rev.npartitions}")
    pasos = []
    rev = aplicar("sin texto (nulo o vacio)", rev,
                  lambda d: d[d["text"].notnull() & (d["text"].str.strip().str.len() > 0)], pasos,
                  "el modelo de sentimiento necesita texto")
    rev = aplicar("fecha nula o invalida", rev,
                  lambda d: d.assign(date=dd.to_datetime(d["date"], errors="coerce")).dropna(subset=["date"]), pasos,
                  "la fecha define la vida del negocio (primera y ultima resena)")
    rev = aplicar("sin business_id", rev,
                  lambda d: d.dropna(subset=["business_id"]), pasos,
                  "sin negocio no se puede unir resena-negocio")
    rev = aplicar("review_id duplicado", rev,
                  lambda d: d.drop_duplicates(subset=["review_id"]), pasos,
                  "una resena duplicada sesga el sentimiento y la linea de tiempo")
    resultado["resenas"] = pasos

    Path("outputs").mkdir(exist_ok=True)
    Path("outputs/conteos_limpieza.json").write_text(json.dumps(resultado, indent=2, ensure_ascii=False))
    print("Guardado en outputs/conteos_limpieza.json")


if __name__ == "__main__":
    main()
