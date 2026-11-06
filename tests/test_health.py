import os
import requests

API_URL = os.environ.get("API_URL", "http://api:5000")


def test_health_responde_200():
    r = requests.get(f"{API_URL}/health", timeout=10)
    assert r.status_code == 200


def test_health_status_ok():
    r = requests.get(f"{API_URL}/health", timeout=10)
    assert r.json()["status"] == "ok"


def test_health_mongo_conectado():
    r = requests.get(f"{API_URL}/health", timeout=10)
    assert r.json()["mongo"] == "conectado"
