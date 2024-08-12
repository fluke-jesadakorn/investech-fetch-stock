import pytest
from fastapi.testclient import TestClient
from app.main import app

# Initialize the TestClient with your FastAPI app
client = TestClient(app)


def test_check_ready():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Ok"}


def test_fetch_and_save_symbols_endpoint(mocker):
    mocker.patch(
        "app.services.fetch_and_save_symbols_1.fetch_and_insert_symbols",
        return_value=None,
    )
    response = client.get("/1")
    assert response.status_code == 200
    assert response.json() == {"message": "Symbols fetched and saved successfully"}


def test_fetch_and_save_news_endpoint(mocker):
    mocker.patch(
        "app.services.fetch_news_2.setup_session_with_proxy", return_value=None
    )
    mocker.patch(
        "app.services.fetch_news_2.fetch_symbols_from_mongo",
        return_value=["AAPL", "GOOGL"],
    )
    mocker.patch("app.services.fetch_news_2.fetch_and_save_news", return_value=None)
    response = client.get("/2")
    assert response.status_code == 200
    assert response.json() == {"message": "News fetched and saved successfully"}


def test_fetch_process_save_news_items_endpoint(mocker):
    mocker.patch(
        "app.services.data_processing_3.fetch_process_save_news_items",
        return_value=None,
    )
    response = client.get("/3")
    assert response.status_code == 200
    assert response.json() == {
        "message": "News items fetched, processed, and saved successfully"
    }


def test_calculate_and_save_predicted_prices_endpoint(mocker):
    mocker.patch(
        "app.services.fetch_price_4.calculate_and_save_predicted_prices",
        return_value=None,
    )
    response = client.get("/4")
    assert response.status_code == 200
    assert response.json() == {
        "message": "Predicted prices calculated and saved successfully"
    }


def test_fetch_and_save_last_prices_endpoint(mocker):
    mocker.patch(
        "app.services.fetch_gap_price_5.fetch_and_save_symbols", return_value=None
    )
    response = client.get("/5")
    assert response.status_code == 200
    assert response.json() == {"message": "Last prices fetched and saved successfully"}
