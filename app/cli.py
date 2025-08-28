# app/cli.py
import argparse, sys
from datetime import date, timedelta

from app.ingest import ingest_many
from app.model import cluster_and_save, best_k_by_silhouette, compute_returns
from app.report import corr_heatmap, cluster_summary 

def main() -> int:
    p = argparse.ArgumentParser(prog="app.cli", description="Project CLI (Mongo-first).")
    sub = p.add_subparsers(dest="cmd", required=True)

    # cluster
    p_cluster = sub.add_parser("cluster", help="Cluster tickers using Mongo data")
    p_cluster.add_argument("--start", required=True)
    p_cluster.add_argument("--end", required=True)       
    p_cluster.add_argument("--k", type=int, required=True)
    p_cluster.add_argument("--out", required=True)
    p_cluster.add_argument("--tickers", nargs="*")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Bulk ingest tickers from a file into Mongo")
    p_ingest.add_argument("--tickers-file", required=True)
    p_ingest.add_argument("--start", required=True)
    p_ingest.add_argument("--end", required=False, help="Default: tomorrow (end-exclusive)")

    # silhouette
    p_sil = sub.add_parser("silhouette", help="Scan k and compute silhouette scores")
    p_sil.add_argument("--start", required=True)
    p_sil.add_argument("--end", required=True)
    p_sil.add_argument("--kmin", type=int, default=2)
    p_sil.add_argument("--kmax", type=int, default=10)
    p_sil.add_argument("--out", required=True)

    # report 
    p_rep = sub.add_parser("report", help="Generate PNG/CSV artifacts")
    p_rep.add_argument("--start", required=True)
    p_rep.add_argument("--end", required=True)
    p_rep.add_argument("--clusters-csv", required=True)

    args = p.parse_args()

    if args.cmd == "cluster":
        try:
            cluster_and_save(args.start, args.end, args.k, args.out, tickers=args.tickers)
            print(f"Saved clusters to {args.out}"); return 0
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr); return 1

    if args.cmd == "ingest":
        try:
            end = args.end or (date.today() + timedelta(days=1)).isoformat()
            with open(args.tickers_file, encoding="utf-8") as f:
                syms = [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
            ok, changed = ingest_many(syms, args.start, end)
            print(f"Ingested {ok} tickers; upserted/updated ~{changed} docs"); return 0
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr); return 1

    if args.cmd == "silhouette":
        try:
            rets = compute_returns(args.start, args.end)
            n = rets.shape[1]
            kmax = min(args.kmax, n - 1)
            kmin = max(args.kmin, 2)
            df = best_k_by_silhouette(rets, kmin, kmax)
            df.to_csv(args.out, index=False)
            print(f"Saved silhouette scan to {args.out}"); return 0
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr); return 1

    if args.cmd == "report":
        try:
            png = corr_heatmap(args.start, args.end)
            csv = cluster_summary(args.clusters_csv)
            print(f"Wrote: {png}\nWrote: {csv}"); return 0
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr); return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
