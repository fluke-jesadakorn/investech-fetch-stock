import pytest
from app.services.fetch_symbol import setup_session, get_news_for_symbol
from app.services.data_processing import convert_to_numbers


def test_setup_session():
    session = setup_session()
    assert session.headers["User-Agent"]
    assert session.headers["Accept"] == "application/json"


def test_convert_to_numbers():
    input_list = ["1,234", "(567)", "89.01", "(23.45)"]
    result = convert_to_numbers(input_list, "")
    assert result == [1234, -567, 89.01, -23.45]


@pytest.mark.parametrize("symbol", ["AOT", "PTT"])
def test_get_news_for_symbol(symbol):
    session = setup_session()
    news_items = get_news_for_symbol(session, symbol)
    assert isinstance(news_items, list)
