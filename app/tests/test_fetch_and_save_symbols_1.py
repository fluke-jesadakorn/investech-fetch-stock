import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from requests import Session
from pymongo import MongoClient
from app.services.fetch_and_save_symbols_1 import (
    fetch_symbol,
    insert_symbols_to_mongo,
    fetch_and_insert_symbols,
)
from app.services.utils import db


class TestFetchAndInsertSymbols(unittest.TestCase):
    @patch("app.services.fetch_and_save_symbols_1.setup_session")
    @patch("app.services.fetch_and_save_symbols_1.fetch_symbol")
    @patch("app.services.fetch_and_save_symbols_1.insert_symbols_to_mongo")
    def test_fetch_and_insert_symbols_success(
        self, mock_insert, mock_fetch, mock_setup_session
    ):
        # Mock the session setup
        mock_session = MagicMock(spec=Session)
        mock_setup_session.return_value = mock_session

        # Mock the fetch_symbol to return a valid DataFrame
        mock_df = pd.DataFrame(
            {
                "symbol": ["ABC", "DEF"],
                "industry": ["Tech", "Finance"],
                "isForeignListing": [False, True],
                "isIFF": [False, False],
                "market": ["SET", "SET"],
                "nameEN": ["Company ABC", "Company DEF"],
                "nameTH": ["บริษัท เอ บี ซี", "บริษัท ดี อี เอฟ"],
                "querySector": ["Tech", "Finance"],
                "remark": ["", ""],
                "sector": ["Technology", "Financials"],
                "securityType": ["Stock", "Stock"],
                "typeSequence": [1, 2],
            }
        )
        mock_fetch.return_value = mock_df

        # Call the function under test
        fetch_and_insert_symbols()

        # Assert that fetch_symbol was called with the correct session
        mock_fetch.assert_called_once_with(mock_session)

        # Assert that insert_symbols_to_mongo was called with the correct DataFrame
        mock_insert.assert_called_once_with(mock_df, db)

    @patch("app.services.fetch_and_save_symbols_1.setup_session")
    @patch("app.services.fetch_and_save_symbols_1.fetch_symbol")
    @patch("app.services.fetch_and_save_symbols_1.insert_symbols_to_mongo")
    def test_fetch_and_insert_symbols_no_data(
        self, mock_insert, mock_fetch, mock_setup_session
    ):
        # Mock the session setup
        mock_session = MagicMock(spec=Session)
        mock_setup_session.return_value = mock_session

        # Mock the fetch_symbol to return an empty DataFrame
        mock_fetch.return_value = pd.DataFrame()

        # Call the function under test
        fetch_and_insert_symbols()

        # Assert that fetch_symbol was called with the correct session
        mock_fetch.assert_called_once_with(mock_session)

        # Assert that insert_symbols_to_mongo was not called since no data is fetched
        mock_insert.assert_not_called()

    @patch("app.services.fetch_and_save_symbols_1.fetch_symbol")
    def test_fetch_symbol_success(self, mock_get):
        # Mock the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "securitySymbols": [
                {
                    "symbol": "ABC",
                    "industry": "Tech",
                    "isForeignListing": False,
                    "isIFF": False,
                    "market": "SET",
                    "nameEN": "Company ABC",
                    "nameTH": "บริษัท เอ บี ซี",
                    "querySector": "Tech",
                    "remark": "",
                    "sector": "Technology",
                    "securityType": "Stock",
                    "typeSequence": 1,
                },
                {
                    "symbol": "DEF",
                    "industry": "Finance",
                    "isForeignListing": True,
                    "isIFF": False,
                    "market": "SET",
                    "nameEN": "Company DEF",
                    "nameTH": "บริษัท ดี อี เอฟ",
                    "querySector": "Finance",
                    "remark": "",
                    "sector": "Financials",
                    "securityType": "Stock",
                    "typeSequence": 2,
                },
            ]
        }
        mock_get.return_value.get.return_value = mock_response

        # Create a session object
        session = Session()

        # Call the function under test
        df = fetch_symbol(session)

        # Check that the data frame is not empty and has correct data
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)
        self.assertIn("symbol", df.columns)
        self.assertIn("industry", df.columns)

    @patch("db")
    def test_insert_symbols_to_mongo(self, mock_db):
        # Mock the DataFrame
        df = pd.DataFrame(
            {
                "symbol": ["ABC", "DEF"],
                "industry": ["Tech", "Finance"],
                "isForeignListing": [False, True],
                "isIFF": [False, False],
                "market": ["SET", "SET"],
                "nameEN": ["Company ABC", "Company DEF"],
                "nameTH": ["บริษัท เอ บี ซี", "บริษัท ดี อี เอฟ"],
                "querySector": ["Tech", "Finance"],
                "remark": ["", ""],
                "sector": ["Technology", "Financials"],
                "securityType": ["Stock", "Stock"],
                "typeSequence": [1, 2],
            }
        )

        # Mock the MongoDB collection and bulk_write method
        mock_collection = MagicMock()
        mock_db.symbols = mock_collection

        # Call the function under test
        insert_symbols_to_mongo(df, mock_db)

        # Assert that bulk_write was called with correct number of operations
        self.assertEqual(mock_collection.bulk_write.call_count, 1)
        operations = mock_collection.bulk_write.call_args[0][0]
        self.assertEqual(len(operations), 2)


if __name__ == "__main__":
    unittest.main()
