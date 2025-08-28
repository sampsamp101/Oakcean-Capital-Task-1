#!/usr/bin/env python3
import os
import sys
import argparse
import pandas as pd
import yfinance as yf
from pymongo import MongoClient, ASCENDING, UpdateOne

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
DB_NAME = "ockean"
COLL_NAME = "prices"  # target collection


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 1-level columns (handle yfinance MultiIndex)."""
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    
    # If the last level is a single ticker (e.g. only "AAPL"), keep level 0 names
    try:
        last = df.columns.get_level_values(-1)
        if len(pd.unique(last)) == 1:  # single ticker
            out = df.copy()
            out.columns = df.columns.get_level_values(0)
            return out
    except Exception:
        pass

    #general case: join tuple parts with "_"
    out = df.copy()
    out.columns = [
        "_".join(str(x) for x in col if x not in (None, ""))
        if isinstance(col, tuple) else str(col)
        for col in out.columns
    ]
    return out


def pick(df: pd.DataFrame, base: str, sym: str) -> pd.Series:
    """Pick a column by base name with tolerant fallbacks."""
    if base in df.columns:
        return df[base]
    alt = f"{base}_{sym}"
    if alt in df.columns:
        return df[alt]
    for c in df.columns:
        if str(c).startswith(base):
            return df[c]
    raise KeyError(f"Missing column '{base}' (have: {list(df.columns)[:8]} ...)")


def main():
    ap = argparse.ArgumentParser(description="Ingest OHLCV from yfinance into MongoDB (upsert, idempotent).")
    ap.add_argument("symbol", help="Ticker, e.g. AAPL or 600000.SS")
    ap.add_argument("start", help="Start date YYYY-MM-DD")
    ap.add_argument("end",   help="End date YYYY-MM-DD (end-exclusive)")
    args = ap.parse_args()

    # 1) Download
    raw = yf.download(args.symbol, start=args.start, end=args.end,
                      interval="1d", auto_adjust=False, progress=False)
    if raw is None or raw.empty:
        print(f"[ERROR] No data from yfinance for {args.symbol} {args.start}..{args.end}", file=sys.stderr)
        return 1

    # 2) Build date column (always from index) and flatten columns
    date = pd.to_datetime(raw.index, errors="coerce")
    try:
        date = date.tz_localize(None)  # no-op if already tz-naive
    except (TypeError, AttributeError):
        pass

    df = flatten_columns(raw.copy())
    df.insert(0, "date", date)
    df = df.dropna(subset=["date"])

    # 3) Select OHLCV safely
    open_s  = pd.to_numeric(pick(df, "Open",   args.symbol), errors="coerce")
    high_s  = pd.to_numeric(pick(df, "High",   args.symbol), errors="coerce")
    low_s   = pd.to_numeric(pick(df, "Low",    args.symbol), errors="coerce")
    close_s = pd.to_numeric(pick(df, "Close",  args.symbol), errors="coerce")
    # Adjusted close can be "Adj Close" or "Adj_Close"; fall back to Close
    try:
        adj_s = pd.to_numeric(pick(df, "Adj Close", args.symbol), errors="coerce")
    except KeyError:
        try:
            adj_s = pd.to_numeric(pick(df, "Adj_Close", args.symbol), errors="coerce")
        except KeyError:
            adj_s = close_s
    vol_s   = pd.to_numeric(pick(df, "Volume", args.symbol), errors="coerce")

    # 4) Mongo upsert (unique index makes it idempotent)
    client = MongoClient(MONGODB_URI, tz_aware=False)
    col = client.get_database(DB_NAME).get_collection(COLL_NAME)
    col.create_index([("ticker", ASCENDING), ("date", ASCENDING)], unique=True)

    ops = []
    for i in range(len(df)):
        # convert to Python datetime per-row (avoids FutureWarning)
        dt = pd.to_datetime(df["date"].iloc[i]).to_pydatetime()
        doc = {
            "ticker": args.symbol,
            "date": dt,
            "open":  float(open_s.iloc[i])  if pd.notna(open_s.iloc[i]) else None,
            "high":  float(high_s.iloc[i])  if pd.notna(high_s.iloc[i]) else None,
            "low":   float(low_s.iloc[i])   if pd.notna(low_s.iloc[i])  else None,
            "close": float(close_s.iloc[i]) if pd.notna(close_s.iloc[i]) else None,
            "adj_close": float(adj_s.iloc[i]) if pd.notna(adj_s.iloc[i]) else None,
            "volume": int(vol_s.iloc[i]) if pd.notna(vol_s.iloc[i]) else None,
        }
        ops.append(UpdateOne({"ticker": doc["ticker"], "date": doc["date"]},
                             {"$set": doc}, upsert=True))

    changed = 0
    if ops:
        res = col.bulk_write(ops, ordered=False)
        changed = (res.upserted_count or 0) + (res.modified_count or 0)

    print(f"Upserted/updated ~{changed} docs into {DB_NAME}.{COLL_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
