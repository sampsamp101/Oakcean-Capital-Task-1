Oakcean Capital – Task 1 (SSE StatArb)

Store Shanghai Stock Exchange (SSE) daily data since 2010 in MongoDB, expose it via a FastAPI endpoint, and keep it updated. (Task 2 scripts for correlations, clustering, and a simple pairs backtest are included.)

Important Requirements before starting

Project is carried out at Python 3.13 exactly. older version of 3.13 might not be supported.

Docker (to run MongoDB locally)

pip install -r requirements.txt

Quick Start.

# 0) open terminal in the project root (Oakcean-Capital-Task-1)

# 1) create & activate a virtual env
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 2) start MongoDB (Docker)
docker run -d --name mongo -p 27017:27017 mongo:6

# 3) create DB index
python -m app.scripts.init_db_indexes

# 4) initial load (edit app/fetch/tickers_sse.csv to add tickers)
python -m app.fetch.update_all --start 2010-01-01 --end 2025-08-18

# 5) run the API
uvicorn app.api.main:app --reload --port 8000

To Test
(run in Browser)
http://127.0.0.1:8000/historical?ticker=600000.SS&start=2020-01-01&end=2020-12-31&fields=date,close,volume

(Optional, for daily upgrade)
python -m app.fetch.update_all

Notes

Ticker format (Yahoo Finance): NNNNNN.SS (e.g., 600519.SS).

Data fields stored: date, open, high, low, close, adj_close, volume.

MongoDB defaults: mongodb://localhost:27017, DB sse_data, collection daily_prices (see app/db.py).

If the API returns [], load data first (step 4).
