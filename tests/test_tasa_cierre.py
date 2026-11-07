"""
Pruebas de la tasa de cierre anualizada (seccion 4 del enunciado):

    Tasa de cierre (%) = cierres / (dias_negocio / 365) * 100

Se verifica la formula pura y luego que cada fila servida por /stats/closure
(calculada por el MapReduce de Spark) cumpla la formula y el minimo de
dias-negocio pedido como parametro.
"""
import os

import pytest
import requests

API_URL = os.environ.get("API_URL", "http://api:5000")


def tasa_cierre_anualizada(cierres, dias_negocio):
    return cierres / (dias_negocio / 365) * 100


def test_formula_un_cierre_en_un_anio_es_100_por_ciento():
    assert tasa_cierre_anualizada(1, 365) == pytest.approx(100.0)


def test_formula_casos_conocidos():
    assert tasa_cierre_anualizada(0, 1000) == 0
    assert tasa_cierre_anualizada(2, 730) == pytest.approx(100.0)
    assert tasa_cierre_anualizada(1, 637) == pytest.approx(57.30, abs=0.01)


@pytest.mark.parametrize("agrupar_por", ["categoria_principal", "geohash"])
def test_stats_closure_cumple_formula_y_minimo(agrupar_por):
    min_dias = 365
    r = requests.get(
        f"{API_URL}/stats/closure",
        params={"por": agrupar_por, "min_dias_negocio": min_dias},
        timeout=30,
    )
    assert r.status_code == 200
    resultados = r.json()["resultados"]
    assert len(resultados) > 0, "el MapReduce no ha escrito tasas en MongoDB"

    for fila in resultados:
        assert fila["dias_negocio"] >= min_dias
        esperado = tasa_cierre_anualizada(fila["cierres"], fila["dias_negocio"])
        assert fila["tasa_cierre_pct"] == pytest.approx(esperado, abs=0.01)
