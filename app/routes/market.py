from fastapi import APIRouter, HTTPException
from app.services.fetch_symbol import setup_session, get_all_market_quote

router = APIRouter()


@router.get("/fetch_market_data")
async def fetch_market_data():
    try:
        session = setup_session()
        get_all_market_quote(session)
        return {"message": "Market data fetched successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
