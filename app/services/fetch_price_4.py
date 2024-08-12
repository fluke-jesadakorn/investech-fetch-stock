from pymongo import MongoClient
from tvDatafeed.main import TvDatafeed, Interval
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
from .utils import db

load_dotenv()

tv = TvDatafeed()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
client2 = MongoClient(mongo_uri)

cache_collection = client2["StockThaiAnalysis"]["HistoricalDataCache"]
processed_collection = db["processed"]
predict_collection = db["predict"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
)


def get_price_on_date(symbol, date, retries=3, backoff_factor=2):
    logging.info(f"Fetching price for {symbol} on {date.date()}")
    cached_data = cache_collection.find_one({"symbol": symbol})

    if cached_data:
        logging.info(f"Using cached data for {symbol}")
        data = pd.DataFrame(cached_data["data"])
        data["datetime"] = pd.to_datetime(data["datetime"])
    else:
        logging.info(f"No cache available, fetching from TV API for {symbol}")
        attempt = 0
        while attempt < retries:
            try:
                data = tv.get_hist(
                    symbol=symbol,
                    exchange="SET",
                    interval=Interval.in_daily,
                    n_bars=5000,
                )
                if data is None or data.empty:
                    logging.error(
                        f"No data returned for {symbol} on exchange 'SET'. Please check the symbol and exchange."
                    )
                    return None
                data["datetime"] = data.index
                data_for_mongo = data.reset_index(drop=True)
                data_for_mongo["datetime"] = data_for_mongo["datetime"].astype(str)
                cache_collection.insert_one(
                    {"symbol": symbol, "data": data_for_mongo.to_dict("list")}
                )
                logging.info(f"Data fetched and cached for {symbol}")
                break
            except Exception as e:
                if "429" in str(e):
                    attempt += 1
                    sleep_time = backoff_factor**attempt
                    logging.warning(
                        f"Rate limit reached. Retrying in {sleep_time} seconds..."
                    )
                    time.sleep(sleep_time)
                elif "Connection to remote host was lost" in str(e):
                    attempt += 1
                    sleep_time = backoff_factor**attempt
                    logging.warning(
                        f"Connection lost. Retrying in {sleep_time} seconds..."
                    )
                    time.sleep(sleep_time)
                else:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    return None

    data_filtered = data[data["datetime"].dt.date == date.date()]
    if not data_filtered.empty:
        logging.info(f"Price found for {symbol} on {date.date()}")
        return data_filtered.iloc[-1]["close"]
    else:
        logging.warning(f"No trading data available for {symbol} on {date.date()}")
        return None


def process_entry(entry):
    symbol = entry["Symbol"]
    date = entry["Datetime"]
    logging.info(f"Processing entry for {symbol} on {date}")

    close_price = get_price_on_date(symbol, date)

    if close_price is not None:
        previous_eps = entry.get("EPS", 0)
        last_eps = entry.get("EPS", 0)
        last_price = close_price

        sum_eps = previous_eps + last_eps
        if sum_eps == 0:
            logging.warning(
                f"Sum of EPS is zero for {symbol} on {date}. Skipping entry."
            )
            return

        price_per_sum_eps = last_price / sum_eps
        addition_price = last_eps * price_per_sum_eps
        predict_price = last_price + addition_price

        predict_entry = {
            "Symbol": symbol,
            "Year": entry["Year"],
            "Quarter": entry["Quarter"],
            "Url": entry["Url"],
            "EPS": entry["EPS"],
            "Datetime": date,
            "ClosePrice": round(close_price, 2),
            "PredictPrice": round(predict_price, 2),
        }

        # Check for duplicate before inserting
        existing_entry = predict_collection.find_one(
            {
                "Symbol": symbol,
                "Year": entry["Year"],
                "Quarter": entry["Quarter"],
                "Datetime": date,
            }
        )

        if not existing_entry:
            try:
                predict_collection.insert_one(predict_entry)
                logging.info(f"Inserted predicted price for {symbol} on {date}")
            except Exception as e:
                logging.error(
                    f"Error inserting predicted price for {symbol} on {date}: {e}"
                )
        else:
            logging.info(f"Predicted price for {symbol} on {date} already exists")
    else:
        logging.warning(
            f"Close price not found for {symbol} on {date}. Skipping entry."
        )


def calculate_and_save_predicted_prices():
    logging.info("Starting calculation of predicted prices")
    processed_data = list(processed_collection.find())

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_entry, entry) for entry in processed_data]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing entry: {e}")

    logging.info("Finished calculating and saving predicted prices")


if __name__ == "__main__":
    logging.info("Calculating and saving predicted prices...")
    calculate_and_save_predicted_prices()
    logging.info("Finished calculating and saving predicted prices")
