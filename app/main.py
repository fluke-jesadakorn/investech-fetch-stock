import logging
from dotenv import load_dotenv
import os
from services.fetch_news_2 import (
    setup_session_with_proxy,
    fetch_symbols_from_mongo,
    fetch_and_save_news,
)
from services.fetch_and_save_symbols_1 import fetch_and_insert_symbols
from services.data_processing_3 import fetch_process_save_news_items
from services.fetch_price_4 import calculate_and_save_predicted_prices
from services.fetch_gap_price_5 import fetch_and_save_symbols

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(asctime)s - %(message)s"
)
logging.getLogger().disabled = False

# Define a function to run the job based on JOB_ID
def run_job(job_id):
    try:
        if job_id == "1":
            fetch_and_insert_symbols()
            logging.info("Symbols fetched and saved successfully")
        elif job_id == "2":
            session = setup_session_with_proxy(proxy_enabled=False)
            symbols = fetch_symbols_from_mongo()
            for symbol in symbols:
                fetch_and_save_news(session, symbol)
            logging.info("News fetched and saved successfully")
        elif job_id == "3":
            fetch_process_save_news_items()
            logging.info("News items fetched, processed, and saved successfully")
        elif job_id == "4":
            calculate_and_save_predicted_prices()
            logging.info("Predicted prices calculated and saved successfully")
        elif job_id == "5":
            fetch_and_save_symbols()
            logging.info("Last prices fetched and saved successfully")
        else:
            logging.error("Invalid job ID")
    except Exception as e:
        logging.error(f"An error occurred while running the job: {e}")


# Main entry point
if __name__ == "__main__":
    import os

    job_id = os.getenv("JOB_ID", "1")  # Get the JOB_ID from environment variables
    run_job(job_id)  # Run the corresponding job
