from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from datetime import datetime
from app.db import col  

app = FastAPI(title="SSE Historical API", version="1.0")

ALLOWED_FIELDS = {"date", "open", "high", "low", "close", "adj_close", "volume", "ticker"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/historical")
def historical(
    ticker: str = Query(..., description="e.g., 600000.SS"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    fields: Optional[str] = Query(None, description="comma-separated fields; default=all"),
):
    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")

    proj = {"_id": 0}
    if fields:
        req = [f.strip() for f in fields.split(",") if f.strip()]
        bad = [f for f in req if f not in ALLOWED_FIELDS]
        if bad:
            raise HTTPException(status_code=400, detail=f"Invalid fields: {bad}")
        for f in req:
            proj[f] = 1
        if "date" not in proj:
            proj["date"] = 1
    else:
        for f in ALLOWED_FIELDS:
            proj[f] = 1

    docs = list(
        col.find(
            {"ticker": ticker, "date": {"$gte": start_dt, "$lte": end_dt}},
            proj,
        ).sort("date", 1)
    )

    for d in docs:
        if "date" in d and hasattr(d["date"], "isoformat"):
            d["date"] = d["date"].isoformat()
    return docs
