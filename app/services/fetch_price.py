from pymongo import MongoClient
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize tvDatafeed
tv = TvDatafeed()

# Setup MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["StockThaiAnalysis"]
cache_collection = db["HistoricalDataCache"]
processed_collection = db["processed"]
predict_collection = db["predict"]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_price_on_date(symbol, date, retries=3, backoff_factor=2):
    cached_data = cache_collection.find_one({"symbol": symbol})
    if cached_data:
        data = pd.DataFrame(cached_data["data"])
        data["datetime"] = pd.to_datetime(data["datetime"])
    else:
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
                break
            except Exception as e:
                if "429" in str(e):
                    attempt += 1
                    sleep_time = backoff_factor**attempt
                    logging.info(
                        f"Retrying in {sleep_time} seconds due to rate limit..."
                    )
                    time.sleep(sleep_time)
                elif "Connection to remote host was lost" in str(e):
                    attempt += 1
                    sleep_time = backoff_factor**attempt
                    logging.info(
                        f"Retrying in {sleep_time} seconds due to connection issue..."
                    )
                    time.sleep(sleep_time)
                else:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    return None

    data_filtered = data[data["datetime"].dt.date == date.date()]
    if not data_filtered.empty:
        return data_filtered.iloc[-1]["close"]
    else:
        logging.info(f"No trading data available for {symbol} on {date.date()}")
        return None


def process_entry(entry):
    symbol = entry["Symbol"]
    date = entry["Datetime"]
    close_price = get_price_on_date(symbol, date)

    if close_price is not None:
        previous_eps = entry.get("EPS", 0)
        last_eps = entry.get("EPS", 0)
        last_price = close_price

        sum_eps = previous_eps + last_eps
        if sum_eps == 0:
            logging.warning(f"Sum of EPS is zero for symbol {symbol} on {date}")
            return

        price_per_sum_eps = last_price / sum_eps
        addition_price = last_eps * price_per_sum_eps
        predict_price = last_price + addition_price

        predict_entry = {
            "Symbol": symbol,
            "Year": entry["Year"],
            "Quarter": entry["Quarter"],
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


def calculate_and_save_predicted_prices():
    processed_data = list(processed_collection.find())

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_entry, entry) for entry in processed_data]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing entry: {e}")


# Run the process
if __name__ == "__main__":
    logging.info("Calculating and saving predicted prices...")
    calculate_and_save_predicted_prices()
    logging.info("Finished calculating and saving predicted prices")
