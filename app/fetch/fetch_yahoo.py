import argparse
import datetime as dt
import pandas as pd
import yfinance as yf
from pymongo import UpdateOne
from app.db import col

def _to_naive(x):
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().replace(tzinfo=None)
    if hasattr(x, "tzinfo") and x.tzinfo is not None:
        return x.replace(tzinfo=None)
    return x

def fetch_history(ticker: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "Open": "open", "High": "high", "Low": "low", "Close": "close",
        "Adj Close": "adj_close", "Volume": "volume"
    })
    df.index.name = "date"
    df.reset_index(inplace=True)
    df["date"] = df["date"].apply(_to_naive)
    df["ticker"] = ticker
    return df[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]]

def upsert_df(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    ops = []
    for _, r in df.iterrows():
        ops.append(UpdateOne(
            {"ticker": r["ticker"], "date": r["date"]},
            {"$set": {
                "open": float(r["open"]) if pd.notna(r["open"]) else None,
                "high": float(r["high"]) if pd.notna(r["high"]) else None,
                "low": float(r["low"]) if pd.notna(r["low"]) else None,
                "close": float(r["close"]) if pd.notna(r["close"]) else None,
                "adj_close": float(r["adj_close"]) if pd.notna(r["adj_close"]) else None,
                "volume": int(r["volume"]) if pd.notna(r["volume"]) else None
            }},
            upsert=True
        ))
    if not ops:
        return 0
    res = col.bulk_write(ops, ordered=False)
    return (res.upserted_count or 0) + (res.modified_count or 0)

def sync_range(ticker: str, start: str, end: str):
    start_dt = dt.datetime.fromisoformat(start).date()
    end_dt = dt.datetime.fromisoformat(end).date()
    df = fetch_history(ticker, start_dt, end_dt)
    n = upsert_df(df)
    print(f"{ticker}: upserted/modified {n} docs in range {start}..{end}")

def sync_latest(ticker: str, lookback_days: int = 10):
    last = col.find({"ticker": ticker}).sort("date", -1).limit(1)
    last_date = None
    for d in last:
        last_date = d.get("date")
    if last_date is None:
        start_dt = dt.date(2010, 1, 1)
    else:
        start_dt = last_date.date() - dt.timedelta(days=lookback_days)
    end_dt = dt.date.today() + dt.timedelta(days=1)
    df = fetch_history(ticker, start_dt, end_dt)
    n = upsert_df(df)
    print(f"{ticker}: upserted/modified {n} docs (from {start_dt} to {end_dt})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    args = parser.parse_args()
    if args.start and args.end:
        sync_range(args.ticker, args.start, args.end)
    else:
        sync_latest(args.ticker)
