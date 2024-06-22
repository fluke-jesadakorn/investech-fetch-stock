import concurrent.futures
from datetime import datetime, timedelta
import logging
from pymongo import UpdateOne
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils import setup_session, db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger().disabled = False


def setup_session_with_proxy(proxy_enabled=False, proxy=None, pool_maxsize=20):
    session = setup_session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(
        pool_connections=pool_maxsize, pool_maxsize=pool_maxsize, max_retries=retries
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if proxy_enabled and proxy:
        session.proxies.update(
            {
                "http": proxy,
                "https": proxy,
            }
        )
    return session


def get_news_for_symbol(session, symbol):
    toDate = datetime.now()
    fromDate = toDate - timedelta(days=5 * 365)
    url = "https://www.set.or.th/api/set/news/search"
    params = {
        "symbol": symbol,
        "fromDate": fromDate.strftime("%d/%m/%Y"),
        "toDate": toDate.strftime("%d/%m/%Y"),
        "keyword": "",
        "lang": "en",
    }
    try:
        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json().get("newsInfoList", [])
        else:
            logging.error(
                f"Request failed with status code {response.status_code} for symbol {symbol}"
            )
            return []
    except Exception as e:
        logging.error(f"Error fetching news for symbol {symbol}: {e}")
        return []


def fetch_symbols_from_mongo(db):
    try:
        symbols_collection = db.symbols
        symbols = symbols_collection.distinct("symbol")
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbols from MongoDB: {e}")
        return []


def save_news_to_mongo(db, news_list):
    news_collection = db.news
    operations = []

    for news in news_list:
        if "(F45)" in news["headline"]:
            if not news_collection.find_one({"url": news.get("url")}):
                operations.append(
                    UpdateOne({"url": news.get("url")}, {"$set": news}, upsert=True)
                )
        if operations:
            try:
                result = news_collection.bulk_write(operations, ordered=False)
                # logging.info(
                #     f"Upserted {result.upserted_count} documents, matched {result.matched_count}"
                # )
            except Exception as e:
                logging.error(f"Error saving news to MongoDB: {e}")


def fetch_and_save_news(session, symbol):
    # logging.info(f"Fetching news for symbol {symbol}")
    news = get_news_for_symbol(session, symbol)
    if news:
        logging.info(f"Saving news for symbol {symbol}")
        save_news_to_mongo(db, news)
    # logging.info(f"News for {symbol} processed")


proxy_enabled = False
proxy = "http://localhost:8080"
pool_maxsize = 20

try:
    test_session = setup_session()
    response = test_session.get("https://www.set.or.th/api/set/news/search", timeout=10)
    if response.status_code == 200:
        logging.info("Connection to the server is successful without proxy.")
    else:
        logging.error(f"Server response without proxy: {response.status_code}")
except Exception as e:
    logging.error(f"Failed to connect to the server without proxy: {e}")

session = setup_session_with_proxy(
    proxy_enabled=proxy_enabled, proxy=proxy, pool_maxsize=pool_maxsize
)
symbols = fetch_symbols_from_mongo(db)

if not symbols:
    logging.error("No symbols fetched from MongoDB")
else:
    max_workers = 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_and_save_news, session, symbol) for symbol in symbols
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"An error occurred: {e}")
