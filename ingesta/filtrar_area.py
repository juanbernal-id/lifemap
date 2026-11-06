"""
Filtra el Yelp Open Dataset completo (business.json, review.json) a una
sola area metropolitana y familia de categorias, para no tener que procesar
los archivos completos en un portatil.

La descarga completa (descargar_kaggle.py) SI queda automatizada; este
paso solo recorta el area de trabajo del equipo, como permite el enunciado
cuando no se puede procesar el archivo completo.

Uso:
    python ingesta/filtrar_area.py
"""
import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

AREA = os.environ.get("AREA_METROPOLITANA", "Tampa")
CATEGORIA = os.environ.get("CATEGORIA", "Restaurants")

RAW = Path("data/raw")
SALIDA = Path("data/filtrado")


def encontrar_archivo(patron):
    candidatos = list(RAW.glob(patron))
    if not candidatos:
        raise FileNotFoundError(f"No se encontro ningun archivo que coincida con {patron} en {RAW}")
    return candidatos[0]


def filtrar_negocios():
    archivo = encontrar_archivo("*business.json")
    negocios_ids = set()
    SALIDA.mkdir(parents=True, exist_ok=True)
    total = 0
    conservados = 0

    with open(archivo, "r", encoding="utf-8") as f_in, \
         open(SALIDA / "negocios.jsonl", "w", encoding="utf-8") as f_out:
        for linea in f_in:
            total += 1
            negocio = json.loads(linea)
            categorias = negocio.get("categories") or ""
            if negocio.get("city", "").strip().lower() != AREA.lower():
                continue
            if CATEGORIA.lower() not in categorias.lower():
                continue
            f_out.write(json.dumps(negocio) + "\n")
            negocios_ids.add(negocio["business_id"])
            conservados += 1

    print(f"Negocios: {total:,} leidos, {conservados:,} conservados (area={AREA}, categoria={CATEGORIA})")
    return negocios_ids


def filtrar_reviews(negocios_ids):
    archivo = encontrar_archivo("*review.json")
    total = 0
    conservados = 0

    with open(archivo, "r", encoding="utf-8") as f_in, \
         open(SALIDA / "reviews.jsonl", "w", encoding="utf-8") as f_out:
        for linea in f_in:
            total += 1
            if total % 2_000_000 == 0:
                print(f"  ...{total:,} reseñas revisadas, {conservados:,} conservadas")
            resena = json.loads(linea)
            if resena.get("business_id") not in negocios_ids:
                continue
            f_out.write(json.dumps(resena) + "\n")
            conservados += 1

    print(f"Reseñas: {total:,} leidas, {conservados:,} conservadas")


def main():
    negocios_ids = filtrar_negocios()
    if not negocios_ids:
        raise SystemExit(
            f"Ningun negocio coincide con area='{AREA}' y categoria='{CATEGORIA}'. "
            "Revisa el valor exacto de 'city' en business.json."
        )
    filtrar_reviews(negocios_ids)


if __name__ == "__main__":
    main()
