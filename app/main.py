from app.services.fetch_symbol import fetch_symbol
from app.services.fetch_news import get_news_for_symbol
from app.services.data_processing import calculatePredictPrice
from app.services.fetch_price import get_price_on_date


def main():
    symbols = fetch_symbol()
    news = get_news_for_symbol()
    processed_data = calculatePredictPrice()
    get_price_on_date()


if __name__ == "__main__":
    main()
