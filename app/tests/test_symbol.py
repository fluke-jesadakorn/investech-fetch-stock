import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pymongo import UpdateOne

from app.services.fetch_and_save_symbols_1 import (
    fetch_symbol,
    insert_symbols_to_mongo,
)  # Import the functions from the module


class TestFetchSymbol(unittest.TestCase):
    @patch("services.fetch_symbol_1.requests.Session.get")
    def test_fetch_symbol_success(self, mock_get):
        # Mock the response to return a successful JSON response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "securitySymbols": [
                {
                    "symbol": "24CS",
                    "industry": "PROPCON",
                    "isForeignListing": False,
                    "isIFF": False,
                    "market": "mai",
                    "nameEN": "Twenty-Four Con & Supply Public Company Limited",
                    "nameTH": "บริษัท ทเวนตี้ โฟร์ คอน แอนด์ ซัพพลาย จำกัด (มหาชน)",
                    "querySector": "",
                    "remark": "",
                    "sector": "",
                    "securityType": "S",
                    "typeSequence": 1,
                }
            ]
        }
        mock_get.return_value = mock_response

        session = MagicMock()
        df = fetch_symbol(session)

        expected_df = pd.DataFrame(
            [
                {
                    "symbol": "24CS",
                    "industry": "PROPCON",
                    "isForeignListing": False,
                    "isIFF": False,
                    "market": "mai",
                    "nameEN": "Twenty-Four Con & Supply Public Company Limited",
                    "nameTH": "บริษัท ทเวนตี้ โฟร์ คอน แอนด์ ซัพพลาย จำกัด (มหาชน)",
                    "querySector": "",
                    "remark": "",
                    "sector": "",
                    "securityType": "S",
                    "typeSequence": 1,
                }
            ]
        )

        pd.testing.assert_frame_equal(df, expected_df)

    @patch("services.fetch_symbol_1.requests.Session.get")
    def test_fetch_symbol_failure(self, mock_get):
        # Mock the response to return a failed status code
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        session = MagicMock()
        df = fetch_symbol(session)

        expected_df = pd.DataFrame()

        pd.testing.assert_frame_equal(df, expected_df)


class TestInsertSymbolsToMongo(unittest.TestCase):
    @patch("services.fetch_symbol_1.db")
    def test_insert_symbols_to_mongo(self, mock_db):
        # Create a DataFrame to be inserted
        df = pd.DataFrame(
            [
                {
                    "symbol": "24CS",
                    "industry": "PROPCON",
                    "isForeignListing": False,
                    "isIFF": False,
                    "market": "mai",
                    "nameEN": "Twenty-Four Con & Supply Public Company Limited",
                    "nameTH": "บริษัท ทเวนตี้ โฟร์ คอน แอนด์ ซัพพลาย จำกัด (มหาชน)",
                    "querySector": "",
                    "remark": "",
                    "sector": "",
                    "securityType": "S",
                    "typeSequence": 1,
                }
            ]
        )

        # Mock the symbols collection and bulk_write method
        mock_symbols_collection = mock_db.symbols
        mock_bulk_write = mock_symbols_collection.bulk_write
        mock_bulk_write.return_value = MagicMock(upserted_count=1, matched_count=0)

        insert_symbols_to_mongo(df, mock_db)

        # Check that the bulk_write method was called correctly
        operations = [
            UpdateOne({"symbol": "24CS"}, {"$set": df.iloc[0].to_dict()}, upsert=True)
        ]
        mock_bulk_write.assert_called_once_with(operations, ordered=False)


if __name__ == "__main__":
    unittest.main()
