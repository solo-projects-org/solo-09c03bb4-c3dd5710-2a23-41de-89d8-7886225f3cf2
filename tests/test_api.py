"""HTTP 层测试：FastAPI TestClient，MQTT 禁用。"""
from __future__ import annotations

import pytest

from app.db import session_scope
from app.engine import record_sample


@pytest.fixture()
def client(mem_db, monkeypatch):
    monkeypatch.setenv("DISABLE_MQTT", "1")
    # 重新导入 app，确保 lifespan 读到环境变量
    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    from fastapi.testclient import TestClient

    with TestClient(main_mod.app) as c:
        yield c


def _sample(startup, seq, weight, stable):
    with session_scope() as s:
        record_sample(
            s, startup_id=startup, seq=seq, weight_kg=weight,
            stable=stable, retained=False,
        )


def test_scan_open_and_complete_flow(client):
    _sample(1, 0, 0.0, True)
    r = client.post("/api/tickets/scan/T-100")
    assert r.status_code == 200
    body = r.json()
    assert body["result"] == "opened"
    ticket_id = body["ticket"]["id"]

    _sample(1, 1, 3200.0, True)
    r = client.get(f"/api/tickets/{ticket_id}")
    assert r.json()["status"] == "completed"
    assert r.json()["gross_weight_kg"] == 3200.0

    # 完成单重扫只回原毛重
    r = client.post("/api/tickets/scan/T-100")
    assert r.json()["result"] == "completed"
    assert r.json()["ticket"]["gross_weight_kg"] == 3200.0


def test_scan_without_zero_returns_409(client):
    r = client.post("/api/tickets/scan/T-101")
    assert r.status_code == 409
    assert "零读数" in r.json()["detail"]


def test_busy_scale_returns_409(client):
    _sample(1, 0, 0.0, True)
    client.post("/api/tickets/scan/T-101")
    r = client.post("/api/tickets/scan/T-102")
    assert r.status_code == 409


def test_list_endpoints(client):
    _sample(1, 0, 0.0, True)
    client.post("/api/tickets/scan/T-103")
    tickets = client.get("/api/tickets").json()
    assert len(tickets) == 1
    samples = client.get("/api/samples").json()
    assert len(samples) == 1
    assert samples[0]["retained"] is False


def test_404_for_unknown_ticket(client):
    assert client.get("/api/tickets/9999").status_code == 404
