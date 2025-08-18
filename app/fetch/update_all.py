import argparse, csv, os
from tqdm import tqdm
from app.fetch.fetch_yahoo import sync_range, sync_latest

DEFAULT_TICKERS_FILE = os.path.join(os.path.dirname(__file__), "tickers_sse.csv")

def read_tickers(path: str):
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = row.get("ticker", "").strip()
            if t:
                yield t

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update all SSE tickers")
    parser.add_argument("--tickers-file", default=DEFAULT_TICKERS_FILE)
    parser.add_argument("--start", default=None)  # YYYY-MM-DD
    parser.add_argument("--end", default=None)    # YYYY-MM-DD
    args = parser.parse_args()

    tickers = list(read_tickers(args.tickers_file))
    print(f"Tickers loaded: {len(tickers)}")

    for t in tqdm(tickers):
        if args.start and args.end:
            sync_range(t, args.start, args.end)
        else:
            sync_latest(t)
