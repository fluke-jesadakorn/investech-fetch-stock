import logging
from datetime import datetime
import requests
from pymongo import MongoClient, errors, ASCENDING
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import db
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
)
logging.getLogger().disabled = False

# Setup MongoDB connection
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
news_collection = db["news"]
processed_collection = db["processed"]


def convert_to_numbers(input_list, url):
    result = []
    for item in input_list:
        if isinstance(item, tuple):
            item = "".join(item)
        item = item.replace(",", "").strip()
        if item and item not in ["Increase", "Profit", "EPS"]:
            try:
                is_negative = item.startswith("(") and item.endswith(")")
                number = (
                    float(item.strip("()")) if "." in item else int(item.strip("()"))
                )
                result.append(-number if is_negative else number)
            except ValueError as e:
                logging.error(
                    f"Error converting {item} to number from URL: {url} - {e}"
                )
    return result


def parse_financial_content(content, news_item, url):
    soup = BeautifulSoup(content, "html.parser")
    content_selector = "raw-html"
    content_block = soup.find("div", {"class": content_selector})
    content = content_block.text if content_block else "N/A"

    logging.info(f"Parsing financial content for URL: {url}")

    is12Months = re.search(r"12 Months|Yearly", content)
    quarter = (
        re.findall(r"12 Months", content)
        if is12Months
        else re.findall(r"Quarter\s[1-3]", content)
    )

    profit_or_loss_list = re.findall(
        r"Increase|Profit|\(?\d{1,3},?\d{1,3},?\d{1,3}\.?\d{1,4}\)?|\(?\d{1,3}\.\d{1,3}\)?|\(?\d{1,3}\)?",
        content,
    )

    years = re.findall(r"20\d{2}|Increase|Profit", content)

    eps_list = re.findall(
        r"EPS|(?:\(\d{1,3}[\.]\d+\)?)|(?:\d{1,2}[\.]\d{1,8})", content
    )

    indexProfitOrLoss = 0

    if "Increase" in profit_or_loss_list:
        indexProfitOrLoss = profit_or_loss_list.index("Increase")
    elif "Profit" in profit_or_loss_list:
        indexProfitOrLoss = profit_or_loss_list.index("Profit")

    if "Increase" in years:
        years = years[: years.index("Increase")]
    elif "Profit" in years:
        years = years[: years.index("Profit")]

    lastIndexProfitOrEPS = len(years)
    profit_or_loss_list = profit_or_loss_list[
        indexProfitOrLoss + 1 : indexProfitOrLoss + lastIndexProfitOrEPS + 1
    ]

    date_str = news_item.get("datetime")
    date_object = datetime.fromisoformat(date_str)

    processed_data = {
        "url": url,
        "symbol": news_item["symbol"],
        "quarter": quarter[0] if quarter else "12 Months",
        "datetime": date_object,
        "PnL": convert_to_numbers(profit_or_loss_list, url),
        "years": convert_to_numbers(years, url),
    }

    indexEPS = eps_list.index("EPS") if "EPS" in eps_list else None
    if indexEPS is not None:
        extracted_eps_list = eps_list[
            indexEPS + 1 : indexEPS + lastIndexProfitOrEPS + 1
        ]
        processed_data["EPS_list"] = (
            convert_to_numbers(extracted_eps_list, url) if extracted_eps_list else [0]
        )

    logging.info(f"Completed parsing financial content for URL: {url}")
    return processed_data


def fetch_url(url):
    retries = 3
    for attempt in range(retries):
        try:
            logging.info(f"Fetching URL: {url}, Attempt: {attempt + 1}")
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                logging.info(f"Successfully fetched URL: {url}")
                return response.text
            else:
                logging.warning(
                    f"Failed to fetch URL: {url} - Status code: {response.status_code}"
                )
        except requests.RequestException as e:
            logging.error(f"Error fetching URL: {url} on attempt {attempt + 1} - {e}")
            if attempt + 1 == retries:
                raise
    return None


def fetch_and_process_news_item(news_item):
    if "(F45)" in news_item["headline"]:
        url = news_item["url"]
        logging.info(f"Fetching and processing news item for URL: {url}")
        content = fetch_url(url)
        if content:
            return parse_financial_content(content, news_item, url)
    else:
        logging.info(f"Skipping news item with headline: {news_item['headline']}")
    return None


def process_data(data):
    logging.info(f"Processing data for {len(data)} items")
    item_dict = {}
    for item in data:
        if (
            "years" in item
            and "symbol" in item
            and isinstance(item["years"], list)
            and item["years"]
        ):
            year = f'{item["years"][0]}'
            symbol = item["symbol"]
            if symbol not in item_dict:
                item_dict[symbol] = {}
            if year not in item_dict[symbol]:
                item_dict[symbol][year] = {}

            quarter_mappings = {
                "Quarter 1": "Q1",
                "Quarter 2": "Q2",
                "Quarter 3": "Q3",
                "12 Months": "Q4",
            }
            quarter_key = quarter_mappings.get(item["quarter"], item["quarter"])

            item_dict[symbol][year][quarter_key] = {
                "PnL": item["PnL"][0] if item["PnL"] else 0,
                "EPS": (
                    item["EPS_list"][0]
                    if "EPS_list" in item and item["EPS_list"]
                    else 0
                ),
                "url": item.get("url"),
                "datetime": item.get("datetime"),
            }

            if item["quarter"] == "Quarter 3":
                item_dict[symbol][year]["Q3_temp"] = {
                    "PnL": item["PnL"][2] if len(item["PnL"]) > 2 else 0,
                    "EPS": (
                        item["EPS_list"][2]
                        if "EPS_list" in item and len(item["EPS_list"]) > 2
                        else 0
                    ),
                }

    for symbol, years_data in item_dict.items():
        for year, quarters in years_data.items():
            if "Q3_temp" in quarters and "Q4" in quarters:
                quarters["Q4"]["PnL"] -= quarters["Q3_temp"]["PnL"]
                quarters["Q4"]["EPS"] -= quarters["Q3_temp"]["EPS"]

            for quarter, value in quarters.items():
                if isinstance(value, dict) and quarter != "Q3_temp":
                    value.update(
                        {
                            "Profit and Loss (PnL)": value["PnL"],
                            "EPS": round(value["EPS"], 4),
                        }
                    )

    logging.info(f"Completed processing data")
    return item_dict


def reshape_data(data):
    logging.info("Reshaping processed data")
    reshaped_data = []
    for symbol, years_data in data.items():
        for year, quarterly_data in years_data.items():
            for quarter, pnl_data in quarterly_data.items():
                if quarter != "Q3_temp":
                    if isinstance(pnl_data, dict):
                        entry = {
                            "Symbol": symbol,
                            "Year": year,
                            "Quarter": quarter,
                            "Profit and Loss (PnL)": pnl_data["Profit and Loss (PnL)"],
                            "Datetime": pnl_data["datetime"],
                            "Url": pnl_data["url"],
                            "EPS": pnl_data["EPS"],
                        }
                        reshaped_data.append(entry)
    logging.info(f"Completed reshaping data into {len(reshaped_data)} entries")
    return reshaped_data


def save_to_db(entries):
    try:
        logging.info(f"Saving {len(entries)} entries to the database")
        # Sort the entries by Symbol, Year, and Datetime before saving
        sorted_entries = sorted(
            entries,
            key=lambda x: (x["Symbol"], x["Year"], x["Datetime"]),
            reverse=False,
        )

        for entry in sorted_entries:
            try:
                # Insert the entry into the MongoDB collection
                inserted = processed_collection.insert_one(entry)
                if inserted.acknowledged:
                    logging.info(
                        f"Inserted {entry['Symbol']} for {entry['Quarter']} successfully"
                    )
            except errors.PyMongoError as e:
                logging.error(f"Error inserting to MongoDB: {e}")

        # Create an index on the fields Symbol, Year, and Datetime to optimize queries
        processed_collection.create_index(
            [("Symbol", ASCENDING), ("Year", ASCENDING), ("Datetime", ASCENDING)]
        )
        logging.info("Created index on Symbol, Year, and Datetime fields in MongoDB")

    except Exception as e:
        logging.error(f"Error sorting and saving entries to MongoDB: {e}")


def fetch_process_save_news_items():
    try:
        logging.info("Starting fetch, process, and save of news items")
        processed_urls = {
            item["Url"]
            for item in processed_collection.find({}, {"Url": 1})
            if "Url" in item
        }
        logging.info(
            f"Fetched {len(processed_urls)} processed URLs from the collection"
        )

        news_items = list(news_collection.find({"url": {"$nin": list(processed_urls)}}))
        logging.info(f"Fetched {len(news_items)} news items from the collection")

    except errors.PyMongoError as e:
        logging.error(f"Error fetching processed URLs or news items: {e}")
        return

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_news_item = {
            executor.submit(fetch_and_process_news_item, news_item): news_item
            for news_item in news_items
        }
        for future in as_completed(future_to_news_item):
            news_item = future_to_news_item[future]
            try:
                result = future.result()
                if result:
                    symbol = result["symbol"]
                    logging.info(f"Processing symbol: {symbol}")
                    processed_symbol_data = process_data([result])
                    reshaped_data = reshape_data(processed_symbol_data)
                    save_to_db(reshaped_data)
            except Exception as e:
                logging.error(f"Error processing news item {news_item['url']}: {e}")

    logging.info("Completed fetch, process, and save of news items")
