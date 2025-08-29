# app/data.py
import os
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from pymongo import MongoClient, ASCENDING, UpdateOne

def _client() -> MongoClient:
    """
    Connect to MongoDB. Defaults to local server.
    """
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    return MongoClient(uri, tz_aware=False)  # store tz-naive datetimes

def prices_collection():
    db = _client().get_database("ockean")
    col = db.get_collection("prices")
    col.create_index([("ticker", ASCENDING), ("date", ASCENDING)], unique=True)
    return col

def upsert_prices(df: pd.DataFrame, ticker: str, col=None) -> int:
    """
    Upsert OHLCV rows into Mongo. Expects columns:
    ['date','Open','High','Low','Close','Adj Close','Volume'].
    """
    if col is None:
        col = prices_collection()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date"])

    ops: list[UpdateOne] = []
    for _, r in df.iterrows():
        doc = {
            "ticker": ticker,
            "date": r["date"].to_pydatetime(),
            "open": float(r["Open"]),
            "high": float(r["High"]),
            "low": float(r["Low"]),
            "close": float(r["Close"]),
            "adj_close": float(r.get("Adj Close", r["Close"])),
            "volume": None if pd.isna(r["Volume"]) else int(r["Volume"]),
        }
        ops.append(
            UpdateOne(
                {"ticker": doc["ticker"], "date": doc["date"]},
                {"$set": doc},
                upsert=True,
            )
        )
    if not ops:
        return 0
    res = col.bulk_write(ops, ordered=False)
    return int((res.upserted_count or 0) + (res.modified_count or 0))

def panel_close(
    start: Optional[str],
    end: Optional[str],
    tickers: Optional[Iterable[str]] = None,
    col=None,
) -> pd.DataFrame:
    """
    Return a wide DataFrame of Close prices from Mongo (index=date, columns=tickers).
    """
    if col is None:
        col = prices_collection()

    q: dict = {}
    if start:
        q.setdefault("date", {})["$gte"] = pd.to_datetime(start).to_pydatetime()
    if end:
        q.setdefault("date", {})["$lt"] = pd.to_datetime(end).to_pydatetime()
    if tickers:
        q["ticker"] = {"$in": list(tickers)}

    rows = list(col.find(q, {"_id": 0, "ticker": 1, "date": 1, "close": 1}))
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close", "ticker"])
    wide = df.pivot(index="date", columns="ticker", values="close").sort_index()
    return wide


def returns_log(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return np.log(s).diff()
