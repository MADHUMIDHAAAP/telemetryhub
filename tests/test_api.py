import os, time, tempfile
os.environ["SIMULATOR"] = "0"; os.environ["DATABASE_URL"] = f"sqlite:///{tempfile.mkdtemp()}/t.db"
from fastapi.testclient import TestClient
from app.main import app

def test_flow():
    with TestClient(app) as c:
        now = time.time()
        for i in range(8):
            assert c.post("/api/ingest", json={"device_id": "T1", "temperature": 12, "humidity": 50, "vibration": .3, "ts": now - 35 + i * 5}).status_code == 200
        assert any("T1" in a["message"] and not a["resolved"] for a in c.get("/api/alerts").json())
        c.post("/api/ingest", json={"device_id": "T1", "temperature": 4, "ts": now})
        assert c.get("/api/alerts").json()[0]["resolved"]
        assert c.get("/api/devices").json()[0]["online"]
        assert "T1" in c.get("/api/export.csv").text
        assert c.post("/api/rules", json={"metric": "bad", "op": ">", "threshold": 1}).status_code == 422
