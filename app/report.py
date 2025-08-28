import os
import pandas as pd
import matplotlib.pyplot as plt
from app.model import compute_returns

def corr_heatmap(start: str, end: str, tickers=None,out_png = "outputs/corr_heatmap.png"):
    rets = compute_returns(start, end, tickers)
    corr = rets.corr().fillna(0.0)
    os.makedirs(os.path.dirname(out_png), exist_ok = True)
    fig = plt.figure(figsize = (10,8))
    plt.imshow(corr.values, interpolation = "nearest")
    plt.title(f"Return Correlations {start} to {end}")
    plt.xticks(range(len(corr.columns)), corr.columns, rotation = 90)
    plt.yticks(range(len(corr.index)), corr.index)
    plt.colorbar()
    plt.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return out_png

def cluster_summary(clusters_csv:str, out_csv="outputs/cluster_sizes.csv"):
    df = pd.read_csv(clusters_csv)
    summary = df.groupby("cluster").size().reset_index(name = "n_tickers").sort_values("cluster")
    os.makedirs(os.path.dirname(out_csv), exist_ok = True)
    summary.to_csv(out_csv, index = False)
    return out_csv

