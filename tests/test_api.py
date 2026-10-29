import os, tempfile
os.environ["DATABASE_PATH"] = os.path.join(tempfile.gettempdir(), "risk_engine_final_poc_tests.db")
from fastapi.testclient import TestClient
from app.main import app, store
client = TestClient(app)

def setup_function():
    store.reset()

def test_health():
    assert client.get("/health").status_code == 200

def test_oversized_order_rejected():
    r = client.post("/orders/evaluate", json={"symbol":"RELIANCE","side":"BUY","quantity":100,"price":2500})
    assert r.status_code == 200
    assert r.json()["order"]["status"] == "REJECTED"

def test_approved_order_can_be_filled():
    r = client.post("/orders/evaluate", json={"symbol":"TCS","side":"BUY","quantity":2,"price":1000})
    assert r.json()["order"]["status"] == "APPROVED"
    oid = r.json()["order"]["id"]
    f = client.post(f"/orders/{oid}/simulate-fill")
    assert f.status_code == 200
    assert f.json()["order"]["status"] == "FILLED"

def test_batch_upload():
    files = {"file": ("orders.csv", "order_id,symbol,side,quantity,price\nA1,TCS,BUY,2,1000\nA2,INFY,BUY,100,1500\n", "text/csv")}
    r = client.post("/orders/batch-evaluate", files=files)
    assert r.status_code == 200
    assert r.json()["rows_processed"] == 2

def test_stress_test():
    r = client.post("/stress-test", json={"shocks":{"TCS":-0.1}})
    assert r.status_code == 200
