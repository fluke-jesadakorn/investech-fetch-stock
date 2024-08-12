import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from .utils import db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
)
logging.getLogger().disabled = False

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["StockThaiAnalysis"]
predict_collection = db["predict"]
last_price_collection = db["last_price"]


def get_cookies_and_headers_with_selenium(driver, symbol):
    try:
        logging.info(f"Retrieving headers for symbol {symbol} using Selenium")
        url = f"https://www.set.or.th/th/market/product/stock/quote/{symbol}/price"
        driver.get(url)

        # Get cookies
        cookies = driver.get_cookies()
        cookie_header = "; ".join(
            [f"{cookie['name']}={cookie['value']}" for cookie in cookies]
        )

        # Get user-agent
        user_agent = driver.execute_script("return navigator.userAgent;")

        headers = {
            "User-Agent": user_agent,
            "Cookie": cookie_header,
        }

        logging.info(f"Successfully retrieved headers for {symbol}")
        return headers

    except Exception as e:
        logging.error(f"Error retrieving headers with Selenium for {symbol}: {e}")
        return None


def fetch_stock_price(symbol, headers):
    api_url = f"https://www.set.or.th/api/set/stock/{symbol}/related-product/o?lang=th"
    logging.info(f"Fetching stock price for {symbol} from {api_url}")

    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            try:
                market_price = data["relatedProducts"][0]["prior"]
                logging.info(
                    f"Successfully fetched market price for {symbol}: {market_price}"
                )
                return market_price
            except (IndexError, KeyError):
                logging.error(f"Failed to parse JSON response for {symbol}")
                return None
        else:
            logging.error(
                f"Error fetching stock price for {symbol}: {response.status_code} - {response.text}"
            )
            return None
    except requests.RequestException as e:
        logging.error(f"Request error fetching stock price for {symbol}: {e}")
        return None


def fetch_and_save_symbols():
    logging.info("Starting the fetch and save symbols process")
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    try:
        documents = predict_collection.find({}, {"Symbol": 1})
        total_documents = predict_collection.count_documents({})
        logging.info(f"Found {total_documents} symbols in the predict collection")

        for doc in documents:
            symbol = doc.get("Symbol")

            if symbol:
                logging.info(f"Processing symbol: {symbol}")
                headers = get_cookies_and_headers_with_selenium(driver, symbol)
                if headers:
                    market_price = fetch_stock_price(symbol, headers)

                    if market_price is not None:
                        data = {"symbol": symbol, "price": market_price}

                        filter = {"symbol": symbol}
                        update_doc = {"$set": data}
                        last_price_collection.update_one(
                            filter, update_doc, upsert=True
                        )
                        logging.info(f"Updated last price for {symbol}: {market_price}")
                    else:
                        logging.warning(f"Market price not found for symbol: {symbol}")
                else:
                    logging.warning(f"Failed to retrieve headers for symbol: {symbol}")
            else:
                logging.warning(f"Missing symbol in document: {doc}")

    except Exception as e:
        logging.error(f"Error fetching and saving symbols: {e}")
    finally:
        driver.quit()
        logging.info("Completed the fetch and save symbols process")
