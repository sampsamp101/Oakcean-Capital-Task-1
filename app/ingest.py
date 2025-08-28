# app/ingest.py
import os
import pandas as pd
import yfinance as yf
from typing import Iterable, Tuple
from pymongo import MongoClient, ASCENDING, UpdateOne

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")

def _col():
    client = MongoClient(MONGODB_URI, tz_aware=False)
    col = client.get_database("ockean").get_collection("prices")
    col.create_index([("ticker", ASCENDING), ("date", ASCENDING)], unique=True)
    return col

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    # single-ticker multiindex -> keep first level
    try:
        last = df.columns.get_level_values(-1)
        if len(pd.unique(last)) == 1:
            out = df.copy(); out.columns = out.columns.get_level_values(0); return out
    except Exception:  # noqa
        pass
    out = df.copy()
    out.columns = ["_".join(str(x) for x in c if x not in (None, "")) if isinstance(c, tuple) else str(c)
                   for c in out.columns]
    return out

def _pick(df: pd.DataFrame, base: str, sym: str) -> pd.Series:
    if base in df.columns: return df[base]
    alt = f"{base}_{sym}"
    if alt in df.columns: return df[alt]
    for c in df.columns:
        if str(c).startswith(base): return df[c]
    raise KeyError(base)

def ingest_symbol(symbol: str, start: str, end: str) -> int:
    raw = yf.download(symbol, start=start, end=end, interval="1d",
                      auto_adjust=False, progress=False)
    if raw is None or raw.empty:
        return 0

    date = pd.to_datetime(raw.index, errors="coerce")
    try: date = date.tz_localize(None)
    except Exception: pass

    df = _flatten_columns(raw.copy())
    df.insert(0, "date", date)
    df = df.dropna(subset=["date"])

    open_s  = pd.to_numeric(_pick(df, "Open",  symbol), errors="coerce")
    high_s  = pd.to_numeric(_pick(df, "High",  symbol), errors="coerce")
    low_s   = pd.to_numeric(_pick(df, "Low",   symbol), errors="coerce")
    close_s = pd.to_numeric(_pick(df, "Close", symbol), errors="coerce")
    try:
        adj_s = pd.to_numeric(_pick(df, "Adj Close", symbol), errors="coerce")
    except Exception:
        try: adj_s = pd.to_numeric(_pick(df, "Adj_Close", symbol), errors="coerce")
        except Exception: adj_s = close_s
    vol_s   = pd.to_numeric(_pick(df, "Volume", symbol), errors="coerce")

    col = _col()
    ops = []
    for i in range(len(df)):
        dt = pd.to_datetime(df["date"].iloc[i]).to_pydatetime()
        doc = {
            "ticker": symbol, "date": dt,
            "open":  None if pd.isna(open_s.iloc[i])  else float(open_s.iloc[i]),
            "high":  None if pd.isna(high_s.iloc[i])  else float(high_s.iloc[i]),
            "low":   None if pd.isna(low_s.iloc[i])   else float(low_s.iloc[i]),
            "close": None if pd.isna(close_s.iloc[i]) else float(close_s.iloc[i]),
            "adj_close": None if pd.isna(adj_s.iloc[i]) else float(adj_s.iloc[i]),
            "volume": None if pd.isna(vol_s.iloc[i]) else int(vol_s.iloc[i]),
        }
        ops.append(UpdateOne({"ticker": symbol, "date": dt}, {"$set": doc}, upsert=True))
    if not ops: return 0
    res = col.bulk_write(ops, ordered=False)
    return int((res.upserted_count or 0) + (res.modified_count or 0))

def ingest_many(symbols: Iterable[str], start: str, end: str) -> Tuple[int, int]:
    changed = 0; ok = 0
    for s in symbols:
        s = s.strip()
        if not s: continue
        n = ingest_symbol(s, start, end)
        if n >= 0: ok += 1; changed += n
    return ok, changed
