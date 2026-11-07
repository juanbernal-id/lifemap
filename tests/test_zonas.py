import os

import requests

API_URL = os.environ.get("API_URL", "http://api:5000")


def test_zones_summary_devuelve_sentimiento_medio():
    r = requests.get(
        f"{API_URL}/zones/summary",
        params={"lat": 27.9506, "lon": -82.4572, "radio_km": 2},
        timeout=30,
    )
    assert r.status_code == 200
    s = r.json()["sentimiento"]
    assert s["resenas"] > 0
    assert -1 <= s["sentimiento_medio"] <= 1
    assert 0 <= s["pct_negativas"] <= 100


def test_parametro_invalido_responde_400():
    r = requests.get(f"{API_URL}/businesses/near",
                     params={"lat": 27.95, "lon": -82.46, "limite": "abc"}, timeout=10)
    assert r.status_code == 400
