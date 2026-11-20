"""
E2.9 - Pruebas del flujo en Jenkins.

Se corren DESPUES de que el Jenkinsfile ya publico un lote de prueba con el
productor y dejo correr el consumidor unos segundos (ver la etapa
"Prueba de streaming" del Jenkinsfile). Aqui solo se verifica el resultado
final a traves de /streaming/estado, para no necesitar pymongo en la imagen
de pruebas.
"""
import os

import requests

API_URL = os.environ.get("API_URL", "http://api:5000")
MIN_PREDICCIONES = int(os.environ.get("MIN_PREDICCIONES_ESPERADAS", "50"))


def test_streaming_clasifico_el_lote_de_prueba():
    r = requests.get(f"{API_URL}/streaming/estado", timeout=10)
    assert r.status_code == 200
    estado = r.json()
    assert estado["predicciones_totales"] >= MIN_PREDICCIONES, (
        f"Se esperaban al menos {MIN_PREDICCIONES} predicciones de streaming, "
        f"hay {estado['predicciones_totales']}. ¿Corrio el productor y el consumidor "
        "en la etapa anterior del pipeline?"
    )


def test_modelo_cargado_es_el_versionado():
    r = requests.get(f"{API_URL}/streaming/estado", timeout=10)
    estado = r.json()
    assert estado["modelo_hash_api"] is not None, "No se encontro outputs/modelo_sentimiento en la API"
    assert estado["modelo_hash_consumidor"] is not None, (
        "El consumidor de streaming no reporto su hash de modelo "
        "(no corrio, o no pudo escribir en streaming_meta)"
    )
    assert estado["modelo_hash_api"] == estado["modelo_hash_consumidor"], (
        "El consumidor cargo un modelo distinto al que esta en el repo "
        "(¿se reentreno por error en vez de reusar el de la Entrega 1?)"
    )
