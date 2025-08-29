# Ockean Capital — Task 1

End-to-end pipeline to **ingest market data** (Yahoo Finance via `yfinance`), **store/reshape data in MongoDB**, **cluster tickers** with `scikit-learn`, and **export reports/plots** to `outputs/`.

- CLI entrypoint: `python -m app.cli`
- Requires a running MongoDB (local or container)

---
## 🗂 Product Structure
├─ app/
│ ├─ init.py
│ ├─ cli.py # main CLI: ingest / cluster / silhouette / report
│ ├─ data.py # Mongo I/O, wide panels, returns helper
│ ├─ ingest.py # yfinance download + Mongo upsert helpers
│ ├─ model.py # clustering + silhouette scan
│ └─ report.py # correlation heatmap + cluster size table
├─ outputs/
│ ├─ clusters.csv
│ ├─ cluster_sizes.csv
│ ├─ corr_heatmap.png
│ └─ silhouette.csv
├─ tickers.txt # (you create) 1 ticker per line, '#' comments allowed
├─ requirements.txt
├─ .gitignore
└─ README.md
---

## ✅ Features

- Pull daily OHLCV time series from **yfinance**.
- Persist time series to **MongoDB** (via **pymongo**).
- Compute features (e.g., returns) and run **KMeans**/**clustering** using **scikit-learn**.
- Visualize series and cluster assignments with **matplotlib**.
- Docker Compose recipe for a local MongoDB instance.

---

## 🧰 Tech Stack

- **Python**: `pandas`, `numpy`, `yfinance`, `pymongo`, `scikit-learn`, `matplotlib`
- **DB**: MongoDB (tested with default `mongodb://localhost:27017/` on mongodb compass)
- Works on Windows, macOS, Linux

---

## 🚀 Quick Start (TL;DR)

# 0) Virtual env + deps
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 1) Mongo connection for this session (match your Compass URL if different)
$env:MONGODB_URI = "mongodb://localhost:27017/"

# 2) A tiny universe of tickers
@"
AAPL
MSFT
NVDA
600000.SS
"@ | Set-Content tickers.txt

# 3) Ingest to Mongo (end is end-exclusive)
python -m app.cli ingest --tickers-file tickers.txt --start 2019-01-01 --end 2025-08-20

# 4) Pick K by silhouette (optional)
python -m app.cli silhouette --start 2023-01-01 --end 2024-12-31 --kmin 2 --kmax 12 --out outputs/silhouette.csv

# 5) Cluster & save labels
python -m app.cli cluster --start 2023-01-01 --end 2024-12-31 --k 6 --out outputs\clusters.csv

# 6) Reports (heatmap + cluster size table)
python -m app.cli report --start 2023-01-01 --end 2024-12-31 --clusters-csv outputs\clusters.csv

# 7) Open the outputs folder (optional)
ii .\outputs

# outputs/ contents
clusters.csv - each ticker's cluster
cluster_sizes.csv - counts per cluster
corr_heatmap.png - correlation matrix of log returns
silhouette.csv - K vs. silhouette score (if you ran the scan)

---
## Testing using MongoDB Compass (Verification)

#1. Connect with the same string you set in the env:
mongodb://localhost:27017/ (or your Atlas URI)

#2. Open database: ockean → collection: prices.

#3. Check documents:
Filter example:
{ "ticker": "AAPL" }

Sort data by date ascending: 
{"date": 1}

#4.The code creates a unique index on {"ticker": "AAPL"}

{"ticker": 1, "date": 1} (unique)

## Commands Reference (CLI)

#Ingest
python -m app.cli ingest --tickers-file tickers.txt --start 2019-01-01 --end 2025-08-20

#Silhouette scan(choose K)
python -m app.cli silhouette --start 2023-01-01 --end 2024-12-31 --kmin 2 --kmax 12 --out outputs/silhouette.csv

#Cluster
python -m app.cli cluster --start 2023-01-01 --end 2024-12-31 --k 6 --out outputs/clusters.csv

#Reports
python -m app.cli report --start 2023-01-01 --end 2024-12-31 --clusters-csv outputs/clusters.csv

## 🧠 How It Works (short)

# 1. Ingest (ingest.py)

- Downloads OHLCV with yfinance

- Flattens multi-index columns when needed

- Upserts to Mongo with a unique (ticker, date) index

# 2. Panel (data.py)

- Builds a wide Close-price panel from Mongo

- Computes log returns per ticker, cleans NaNs/inf

# 3. Model (model.py)

- Standardizes each ticker's return vector

- KMeans clustering

- Silhouette scan helper for picking k

# 4. Report (report.py)

- Correlation heatmap of returns

- Cluster size CSV

## 🛠 Troubleshooting (Wip)

# No data appearing in Compass

- Ensure MONGODB_URI matches Compass connection (local default: mongodb://localhost:27017/).

- Symbols may require suffixes (.SS, .HK); widen your date range.

# “Need at least 3 tickers”

In your window, fewer than 3 tickers have full return series. Ingest more symbols or extend dates.

# k too large

- k can't exceed n_tickers - 1 after cleaning. reduce k or ingest more

# Plots not showing

# reports save directly to outputs/ as files. Open the PNG/CSVs from there.

