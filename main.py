import logging
import concurrent.futures
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os
from app.services.fetch_news_2 import (
    setup_session_with_proxy,
    fetch_symbols_from_mongo,
    fetch_and_save_news,
)
from app.services.fetch_and_save_symbols_1 import fetch_and_insert_symbols
from app.services.data_processing_3 import fetch_process_save_news_items

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(asctime)s - %(message)s"
)
logging.getLogger().disabled = False

# Initialize FastAPI
app = FastAPI()
router = APIRouter()


@router.get("/")
def check_ready():
    return JSONResponse(
        status_code=200,
        content={"message": "Ok"},
    )


@router.get("/fetch_and_save_symbols")
def fetch_and_save_symbols_endpoint():
    try:
        fetch_and_insert_symbols()
        return JSONResponse(
            status_code=200,
            content={"message": "Symbols fetched and saved successfully"},
        )
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request"
        )


@router.get("/fetch_and_save_news")
def fetch_and_save_news_endpoint(
    proxy_enabled: bool = False, proxy: str = None, pool_maxsize: int = 20
):
    session = setup_session_with_proxy(
        proxy_enabled=proxy_enabled, proxy=proxy, pool_maxsize=pool_maxsize
    )
    symbols = fetch_symbols_from_mongo()

    if not symbols:
        logging.error("No symbols fetched from MongoDB")
        return JSONResponse(
            status_code=500,
            content={"message": "No symbols fetched from MongoDB"},
        )
    else:
        max_workers = 10

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(fetch_and_save_news, session, symbol)
                for symbol in symbols
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"An error occurred: {e}")

        return JSONResponse(
            status_code=200,
            content={"message": "News fetched and saved successfully"},
        )


@router.get("/fetch_process_save_news_items")
def fetch_process_save_news_items_endpoint():
    try:
        fetch_process_save_news_items()
        return JSONResponse(
            status_code=200,
            content={
                "message": "News items fetched, processed, and saved successfully"
            },
        )
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(
            status_code=500, detail="An error occurred while processing the request"
        )


app.include_router(router)

# Run the application
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
