"""
Descarga automatica del Yelp Open Dataset desde Kaggle.

Requiere credenciales de Kaggle (kaggle.json en ~/.kaggle/ o
variables de entorno KAGGLE_USERNAME / KAGGLE_KEY).

Uso:
    python ingesta/descargar_kaggle.py
"""
import os
import zipfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATASET = "yelp-dataset/yelp-dataset"
DESTINO = Path("data/raw")


def main():
    DESTINO.mkdir(parents=True, exist_ok=True)

    # Import diferido: kaggle lee las credenciales al importar.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    print(f"Descargando {DATASET} en {DESTINO}...")
    api.dataset_download_files(DATASET, path=str(DESTINO), unzip=False)

    zips = list(DESTINO.glob("*.zip"))
    if not zips:
        raise FileNotFoundError("No se encontro el zip descargado de Kaggle")

    zip_path = zips[0]
    print(f"Descomprimiendo {zip_path.name}...")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(DESTINO)

    print("Archivos disponibles en data/raw:")
    for f in sorted(DESTINO.glob("*")):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
