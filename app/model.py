# app/model.py
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from app.data import panel_close, returns_log
from sklearn.metrics import silhouette_score

def compute_returns(start: str, end: str, tickers=None) -> pd.DataFrame:
    prices = panel_close(start, end, tickers=tickers)
    if prices.empty: raise RuntimeError("No prices loaded from Mongo.")
    rets = prices.apply(returns_log).dropna(axis=1, how="all").dropna(axis=0, how="any")
    return rets

def best_k_by_silhouette(rets: pd.DataFrame, k_min=2, k_max=10) -> pd.DataFrame:

    n = rets.shape[1]
    if n < 3:
        raise ValueError(f"Need at least 3 tickers after cleaning; got {n}")

    X = rets.T.values
    X = (X - X.mean(axis=1, keepdims=True)) / (X.std(axis=1, keepdims=True) + 1e-9)

    k_hi = min(int(k_max), n - 1) #ensure that it doesn't exceed n - 1
    k_low = min(int(k_min), 2) #set the minimum to be 2    

    if k_low > k_hi:
        raise ValueError(f"k range invalid after capping: {k_low} to {k_hi} with n = {n}")

    rows = []
    for k in range(k_low, k_hi + 1):
        km = KMeans(n_clusters=k, n_init=25, random_state=42)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels) 
        rows.append({"k": k, "silhouette": float(score)})
    return pd.DataFrame(rows)

def cluster_and_save(start: str, end: str, k: int, out_csv: str, tickers=None) -> pd.DataFrame:
    prices = panel_close(start, end, tickers=tickers)
    if prices.empty:
        raise RuntimeError("No prices loaded from Mongo for the given window/tickers.")

    # log returns per ticker
    rets = prices.apply(returns_log)
    # drop all-NaN rows/cols
    rets = rets.dropna(axis=1, how="all").dropna(axis=0, how="any")

    if rets.shape[1] < int(k):
        raise ValueError(f"Not enough tickers to form {k} clusters (got {rets.shape[1]}).")

    X = rets.T.values  # n_tickers x n_time
    X = (X - X.mean(axis=1, keepdims=True)) / (X.std(axis=1, keepdims=True) + 1e-9)

    kmeans = KMeans(n_clusters=int(k), n_init=25, random_state=42)
    labels = kmeans.fit_predict(X)

    res = pd.DataFrame({"ticker": rets.columns, "cluster": labels}).sort_values(["cluster", "ticker"])
    res.to_csv(out_csv, index=False)
    return res
