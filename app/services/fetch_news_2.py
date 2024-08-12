from datetime import datetime, timedelta
import logging
from pymongo import UpdateOne
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Optional, Dict, Any, Union
from dataclasses import dataclass
from .utils import setup_session, db

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
)
logging.getLogger().disabled = False


@dataclass
class NewsItem:
    url: str
    datetime: str
    headline: str
    id: str
    isTodayNews: bool
    lang: str
    marketAlertTypeId: Optional[Union[int, None]]
    percentPriceChange: Optional[Union[float, None]]
    product: str
    source: str
    symbol: str
    tag: str
    viewClarification: Optional[str]


def setup_session_with_proxy(
    proxy_enabled: bool = False, proxy: Optional[str] = None, pool_maxsize: int = 20
) -> Session:
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
        logging.info(f"Proxy enabled: {proxy}")
    else:
        logging.info("Proxy not enabled or not provided")

    logging.info("HTTP session setup complete")
    return session


def get_news_for_symbol(session: Session, symbol: str) -> List[NewsItem]:
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

    logging.info(
        f"Fetching news for symbol {symbol} from {fromDate.strftime('%d/%m/%Y')} to {toDate.strftime('%d/%m/%Y')}"
    )

    try:
        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            logging.info(f"Successfully fetched news for symbol {symbol}")
            news_data = response.json().get("newsInfoList", [])
            return [NewsItem(**news) for news in news_data]
        else:
            logging.error(
                f"Failed to fetch news for symbol {symbol}: Status code {response.status_code}"
            )
            return []
    except Exception as e:
        logging.error(f"Error fetching news for symbol {symbol}: {e}")
        return []


def fetch_symbols_from_mongo() -> List[str]:
    try:
        logging.info("Fetching symbols from MongoDB")
        symbols_collection = db.symbols
        symbols = symbols_collection.distinct("symbol")
        logging.info(f"Fetched {len(symbols)} symbols from MongoDB")
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbols from MongoDB: {e}")
        return []


def save_news_to_mongo(news_list: List[NewsItem]) -> None:
    news_collection = db.news
    operations = []

    logging.info(f"Preparing to save {len(news_list)} news items to MongoDB")

    for news in news_list:
        if "(F45)" in news.headline:
            if not news_collection.find_one({"url": news.url}):
                operations.append(
                    UpdateOne({"url": news.url}, {"$set": news.__dict__}, upsert=True)
                )
                logging.debug(f"Prepared update operation for news item: {news.url}")

    if operations:
        try:
            result = news_collection.bulk_write(operations, ordered=False)
            logging.info(
                f"Bulk write complete: Inserted/Updated {result.modified_count} documents."
            )
        except Exception as e:
            logging.error(f"Error saving news to MongoDB: {e}")
    else:
        logging.warning("No news items to update in MongoDB")


def fetch_and_save_news(session: Session, symbol: str) -> None:
    logging.info(f"Starting news fetch and save process for symbol {symbol}")
    news = get_news_for_symbol(session, symbol)
    if news:
        logging.info(f"Saving {len(news)} news items for symbol {symbol} to MongoDB")
        save_news_to_mongo(news)
    else:
        logging.warning(f"No news items found for symbol {symbol}")
    logging.info(f"Completed news fetch and save process for symbol {symbol}")
