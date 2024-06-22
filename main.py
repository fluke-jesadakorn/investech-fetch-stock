from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
import requests
import random
from pymongo import MongoClient
from bs4 import BeautifulSoup
import re
import logging
from tvdatafeed.tvDatafeed import TvDatafeed, Interval
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.getLogger().disabled = True

tv = TvDatafeed()
client = MongoClient("mongodb://localhost:27017/")
db = client["StockThaiAnalysis2"]
cache_collection = db["HistoricalDataCache"]
product_collection = db["Product"]


def setup_session():
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15",
    ]
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(user_agents),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.set.or.th/th/market/get-quote/stock/",
        }
    )
    session.get("https://www.set.or.th/th/market/get-quote/stock/")
    return session


def fetch_data(url, session, retries=3):
    for _ in range(retries):
        response = session.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning(f"Retry {_ + 1} for URL: {url}")
    logging.error(
        f"Failed to retrieve data after {retries} attempts: Status code {response.status_code}"
    )
    return None


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
    response = session.get(url, params=params)
    if response.status_code == 200:
        return response.json().get("newsInfoList", [])
    else:
        logging.error(f"Request failed with status code {response.status_code}")
        return []


def convert_to_numbers(input_list, url):
    result = []
    for item in input_list:
        if not isinstance(item, str):
            continue

        item = item.replace(",", "").strip()

        if not item or item in ["Increase", "Profit", "EPS"]:
            continue

        try:
            is_negative = item.startswith("(") and item.endswith(")")
            item = item.strip("()")
            number = float(item) if "." in item else int(item)
            if is_negative:
                number = -number
            result.append(number)
        except ValueError as e:
            logging.error(f"Error converting {item} to number from URL: {url} - {e}")

    return result


def parse_financial_content(content, news_item, url):
    soup = BeautifulSoup(content, "html.parser")
    content_selector = "raw-html"
    content_block = soup.find("div", {"class": content_selector})
    content = content_block.text if content_block else "N/A"

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

    indexProfitOrLoss: int = 0

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
            convert_to_numbers(eps_list, url) if eps_list else [0]
        )

    return processed_data


def process_news_items(news_items, symbol_data):
    processed_data = []
    for news_item in news_items:
        if "(F45)" in news_item["headline"]:
            news_item.update(symbol_data)
            url = news_item["url"]
            response = requests.get(url)
            if response.status_code == 200:
                content = response.text
                data = parse_financial_content(content, news_item, url)
                processed_data.append(data)
    return processed_data


def processDataQ1toQ4(data):
    itemDict = {}
    for item in data:
        if (
            "years" in item
            and "symbol" in item
            and isinstance(item["years"], list)
            and item["years"]
        ):
            year = f'{item["years"][0]}'
            symbol = item["symbol"]
            if symbol not in itemDict:
                itemDict[symbol] = {}
            if year not in itemDict[symbol]:
                itemDict[symbol][year] = {}
            if "quarter" in item:
                quarter_mappings = {
                    "Quarter 1": "Q1",
                    "Quarter 2": "Q2",
                    "Quarter 3": "Q3",
                    "12 Months": "Q4",
                }
                quarter_key = quarter_mappings.get(item["quarter"], item["quarter"])

                itemDict[symbol][year][quarter_key] = {
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
                    itemDict[symbol][year]["tempQ3"] = (
                        item["PnL"][2] if len(item["PnL"]) > 2 else 0
                    )
                    itemDict[symbol][year]["tempEPSQ3"] = (
                        item["EPS_list"][2]
                        if "EPS_list" in item and len(item["EPS_list"]) > 2
                        else 0
                    )
    return itemDict


def calculateQuarterlyDifferences(data):
    for symbol, years_data in data.items():
        if not isinstance(years_data, dict):
            print(f"Invalid data format for symbol: {symbol}. Skipping...")
            continue
        for year, quarters in years_data.items():
            if "tempQ3" in quarters and "Q4" in quarters:
                quarters["Q4"]["PnL"] = quarters["Q4"]["PnL"] - quarters["tempQ3"]
                quarters["Q4"]["EPS"] = quarters["Q4"]["EPS"] - quarters["tempEPSQ3"]

            all_quarters = [
                (year, q, value)
                for q, value in quarters.items()
                if q in ["Q1", "Q2", "Q3", "Q4"] and isinstance(value, dict)
            ]

            all_quarters.sort()
            prev_pnl = None

            for year, quarter, value in all_quarters:

                pnl = value["PnL"]

                pprint(f"{symbol} {year} {quarter} {pnl}")
                if pnl is not None:
                    if prev_pnl is None:
                        diff = 0
                        percentage_diff = 0
                    else:
                        diff = pnl - prev_pnl
                        percentage_diff = (
                            (diff / prev_pnl * 100) if prev_pnl != 0 else None
                        )
                    data[symbol][year][quarter] = {
                        "Profit and Loss (PnL)": pnl,
                        "PnLDiff": diff,
                        "%PnLDiff": (
                            round(percentage_diff, 2)
                            if percentage_diff is not None
                            else None
                        ),
                        "Url": value.get("url", ""),
                        "datetime": value.get("datetime", ""),
                        "EPS": value.get("EPS"),
                    }
                prev_pnl = pnl
    return data


def reshapeDataForMongoDB(data):
    reshaped_data = []
    for symbol, years_data in data.items():
        for year, quarterly_data in years_data.items():
            for quarter, pnl_data in quarterly_data.items():
                if quarter not in ["tempQ3"]:
                    if isinstance(pnl_data, dict):
                        entry = {
                            "Symbol": symbol,
                            "Year": year,
                            "Quarter": quarter,
                            "Profit and Loss (PnL)": pnl_data["Profit and Loss (PnL)"],
                            "PnLDiff": pnl_data["PnLDiff"],
                            "PnL%Diff": pnl_data["%PnLDiff"],
                            "Datetime": pnl_data["datetime"],
                            "Url": pnl_data["Url"],
                            "EPS": round(pnl_data["EPS"], 4),
                        }
                        reshaped_data.append(entry)
                    else:
                        print(f"Expected dictionary, got {type(pnl_data)}: {pnl_data}")
    return reshaped_data


def get_price_on_date(symbol, date):
    cached_data = cache_collection.find_one({"symbol": symbol})
    if cached_data:
        data = pd.DataFrame(cached_data["data"])
        data["datetime"] = pd.to_datetime(data["datetime"])
    else:
        try:
            data = tv.get_hist(
                symbol=symbol, exchange="SET", interval=Interval.in_daily, n_bars=5000
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
        except Exception as e:
            logging.error(f"Exception occurred while fetching data for {symbol}: {e}")
            return None

    data_filtered = data[data["datetime"].dt.date == date.date()]
    if not data_filtered.empty:
        return data_filtered.iloc[-1]["close"]
    else:
        logging.info(f"No trading data available for {symbol} on {date.date()}")
    return None


def process_symbol_data(symbol_data, session):
    symbol = symbol_data["symbol"]
    news = get_news_for_symbol(session, symbol)
    processed_news = process_news_items(news, symbol_data)
    processed_dict = processDataQ1toQ4(processed_news)
    calculated_differences = calculateQuarterlyDifferences(processed_dict)
    reshaped_data = reshapeDataForMongoDB(calculated_differences)
    last_price = None

    dataList = []
    for doc in reshaped_data:
        date = doc.get("Datetime", None)
        price = get_price_on_date(symbol, date)
        doc["Price"] = round(price, 2)
        if last_price is not None:
            price_diff = price - last_price if price is not None else None
            percent_price_diff = (
                (price_diff / last_price * 100) if last_price != 0 else None
            )
            doc["PriceDiff"] = round(price_diff, 2) if price_diff is not None else None
            doc["%PriceDiff"] = (
                round(percent_price_diff, 2) if percent_price_diff is not None else None
            )
        last_price = price
        try:
            dataList.append(doc)
            logging.info(f"Inserted document for {symbol} at {date}")
        except Exception as e:
            logging.error(f"Failed to insert document for {symbol} at {date}: {e}")
    calculatePredictPrice(dataList)


def calculatePredictPrice(data: list):

    data.sort(key=lambda x: (x["Year"], x["Quarter"]))

    if not data:
        return None

    for index, item in enumerate(data):

        previous_eps = data[index - 1].get("EPS", 0) if index > 0 else 0
        last_eps = item.get("EPS", 0) if item else 0
        last_price = item.get("Price", 0) if item else 0
        sum_eps = previous_eps + last_eps
        if sum_eps == 0:
            return None

        price_per_sum_eps = last_price / sum_eps
        addition_price = last_eps * price_per_sum_eps
        predict_price = last_price + addition_price
        item["PredictPrice"] = (
            round(predict_price, 2) if predict_price is not None else None
        )
        inserted = product_collection.insert_one(item)
        if inserted.acknowledged:
            print(f"insert {item['Symbol']} and {item['Quarter']} success")


def get_all_market_quote(session):
    data_url = "https://www.set.or.th/api/set/stock/list"
    data_json = fetch_data(data_url, session)
    if data_json:
        symbols = pd.DataFrame(data_json.get("securitySymbols", []))
        if not symbols.empty:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(
                        process_symbol_data, symbol_data, session
                    ): symbol_data["symbol"]
                    for symbol_data in symbols.to_dict("records")
                }
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error processing data for symbol {symbol}: {e}")


def main():
    session = setup_session()
    get_all_market_quote(session)


if __name__ == "__main__":
    main()
