from pprint import pprint
import logging
from dotenv import load_dotenv
import pandas as pd
from pymongo import UpdateOne
from .utils import setup_session, db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(asctime)s - %(message)s"
)
logging.getLogger().disabled = True


def fetch_symbol(session):
    data_url = "https://www.set.or.th/api/set/stock/list"
    response = session.get(data_url)
    if response.status_code == 200:
        data_json = response.json()
        pprint(data_json)
        return pd.DataFrame(data_json.get("securitySymbols", []))
    else:
        logging.error(f"Failed to fetch symbols: Status code {response.status_code}")
        return pd.DataFrame()


def insert_symbols_to_mongo(df, db):
    symbols_collection = db.symbols
    operations = []
    for index, row in df.iterrows():
        operations.append(
            UpdateOne({"symbol": row["symbol"]}, {"$set": row.to_dict()}, upsert=True)
        )
    if operations:
        result = symbols_collection.bulk_write(operations, ordered=False)
        logging.info(
            f"Upserted {result.upserted_count} documents, matched {result.matched_count}"
        )


def fetch_and_insert_symbols():
    session = setup_session()
    df_symbols = fetch_symbol(session)
    insert_symbols_to_mongo(df_symbols, db)