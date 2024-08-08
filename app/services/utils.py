import random
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
# mongo_client = MongoClient("mongodb://localhost:27017/")
db = client["StockThaiAnalysis"]


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
