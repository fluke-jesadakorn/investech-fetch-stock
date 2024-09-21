import logging
import pandas as pd
from pymongo import UpdateOne
from requests import Session
from typing import List, Dict, Any
from .utils import setup_session, db
from pymongo.database import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
)

logging.getLogger().disabled = False


def fetch_symbol(session: Session) -> pd.DataFrame:
    """
    Fetches symbols from the SET API and returns a DataFrame.

    Args:
        session (Session): The session object used to make HTTP requests.

    Returns:
        pd.DataFrame: A DataFrame containing the symbol data.
    """
    data_url: str = "https://www.set.or.th/api/set/stock/list"
    logging.info(f"Fetching symbols from {data_url}")

    response = session.get(data_url)
    if response.status_code == 200:
        logging.info("Successfully fetched symbols data from API")
        data_json: Dict[str, Any] = response.json()
        symbols: List[Dict[str, Any]] = data_json.get("securitySymbols", [])

        # Normalize and convert the data to a DataFrame
        df: pd.DataFrame = pd.DataFrame(symbols)
        logging.info(f"Fetched {len(df)} symbols from the API")

        # Ensure consistency in the DataFrame's structure and types
        df["isForeignListing"] = df["isForeignListing"].astype(bool)
        df["isIFF"] = df["isIFF"].astype(bool)

        # Ensure all necessary fields exist
        required_fields: List[str] = [
            "symbol",
            "industry",
            "isForeignListing",
            "isIFF",
            "market",
            "nameEN",
            "nameTH",
            "querySector",
            "remark",
            "sector",
            "securityType",
            "typeSequence",
        ]
        for field in required_fields:
            if field not in df.columns:
                df[field] = None
                logging.warning(
                    f"Field '{field}' is missing in the API response, setting as None"
                )

        logging.info("Completed processing symbols data")
        return df
    else:
        logging.error(
            f"Failed to fetch symbols: Status code {response.status_code}, URL: {data_url}"
        )
        return pd.DataFrame()


def insert_symbols_to_mongo(
    df: pd.DataFrame, db: Database
) -> None:  # Update type hint to Database
    """
    Inserts or updates symbols in the MongoDB collection.

    Args:
        df (pd.DataFrame): The DataFrame containing symbol data to insert or update.
        db (Database): The MongoDB database instance to interact with.
    """
    symbols_collection = db.symbols
    operations: List[UpdateOne] = []

    logging.info(
        f"Preparing to insert/update {len(df)} symbols in the MongoDB collection"
    )

    for _, row in df.iterrows():
        document: Dict[str, Any] = {
            "symbol": row["symbol"],
            "industry": row.get("industry", ""),
            "isForeignListing": row.get("isForeignListing", False),
            "isIFF": row.get("isIFF", False),
            "market": row.get("market", ""),
            "nameEN": row.get("nameEN", ""),
            "nameTH": row.get("nameTH", ""),
            "querySector": row.get("querySector", ""),
            "remark": row.get("remark", ""),
            "sector": row.get("sector", ""),
            "securityType": row.get("securityType", ""),
            "typeSequence": int(row.get("typeSequence", 0)),
        }

        operations.append(
            UpdateOne({"symbol": document["symbol"]}, {"$set": document}, upsert=True)
        )

    if operations:
        logging.info(f"Executing bulk write with {len(operations)} operations")
        result = symbols_collection.bulk_write(operations, ordered=False)
        logging.info(
            f"Bulk write completed: {result.upserted_count} upserted, "
            f"{result.matched_count} matched, {result.modified_count} modified"
        )
    else:
        logging.warning("No operations to execute in bulk write")


def fetch_and_insert_symbols() -> None:
    """
    Fetches symbols using a session and inserts them into the MongoDB collection.
    """
    logging.info("Starting fetch and insert symbols process")

    session: Session = setup_session()
    logging.info("HTTP session set up successfully")

    df_symbols: pd.DataFrame = fetch_symbol(session)

    if not df_symbols.empty:
        insert_symbols_to_mongo(df_symbols, db)
        logging.info(
            f"Inserted {len(df_symbols)} symbols into the database successfully."
        )
    else:
        logging.warning("No symbols fetched, nothing to insert into the database.")

    logging.info("Fetch and insert symbols process completed")
