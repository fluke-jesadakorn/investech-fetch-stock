from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_fetch_market_data():
    response = client.get("/fetch_market_data")
    assert response.status_code == 200
    assert response.json() == {"message": "Market data fetched successfully"}
