import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from src.common import PROCESSED_DIR, TABLES_DIR, CHARTS_DIR, find_beam_output

K_RANGE = range(2, 9)

# Gợi ý hành động marketing cho từng nhóm
ACTIONS = {
    "Khách VIP": "Chương trình thành viên, ưu đãi riêng, chăm sóc 1-1, ưu tiên hàng mới.",
    "Khách đã rời bỏ": "Email win-back, mã giảm giá có hạn, khảo sát lý do ngừng mua.",
    "Khách thường xuyên": "Bán chéo / bán thêm, tích điểm để nâng lên hạng VIP.",
    "Khách mới / mua ít": "Chuỗi email chào mừng, gợi ý sản phẩm bán chạy, ưu đãi đơn thứ 2.",
    "Khách phổ thông": "Khuyến mãi theo mùa, combo sản phẩm.",
}


def build_features(df):
    """RFM -> log1p(F, M) -> chuẩn hóa. Trả về ma trận X đã chuẩn hóa."""
    features = df[["Recency", "Frequency", "Monetary"]].clip(lower=0).copy()
    # Log transform giảm ảnh hưởng lệch phải của Frequency/Monetary.
    for col in ["Frequency", "Monetary"]:
        features[col] = np.log1p(features[col])
    return StandardScaler().fit_transform(features)


def evaluate_k(X):
    rows = []
    for k in K_RANGE:
        model = KMeans(n_clusters=k, random_state=42, n_init=10).fit(X)
        rows.append({"k": k, "inertia": model.inertia_, "silhouette": silhouette_score(X, model.labels_)})
    return pd.DataFrame(rows)


def name_clusters(summary):
    """Đặt tên cụm: chi nhiều nhất = VIP, lâu không mua nhất = rời bỏ, còn lại theo Frequency."""
    if len(summary) > 5:
        return {cid: f"Cụm {cid}" for cid in summary.index}
    names = {}
    remaining = list(summary.index)
    vip = summary["Monetary"].idxmax()
    names[vip] = "Khách VIP"
    remaining.remove(vip)
    lost = summary.loc[remaining, "Recency"].idxmax()
    names[lost] = "Khách đã rời bỏ"
    remaining.remove(lost)
    ordered = summary.loc[remaining].sort_values("Frequency", ascending=False).index
    labels = {
        1: ["Khách phổ thông"],
        2: ["Khách thường xuyên", "Khách mới / mua ít"],
        3: ["Khách thường xuyên", "Khách phổ thông", "Khách mới / mua ít"],
    }.get(len(ordered), [])
    for cid, label in zip(ordered, labels):
        names[cid] = label
    return names


def load_rfm():
    df = pd.read_csv(find_beam_output("customer_aggregates"))
    df["last_purchase"] = pd.to_datetime(df["last_purchase"], errors="coerce")
    reference_date = df["last_purchase"].max() + pd.Timedelta(days=1)
    df["Recency"] = (reference_date - df["last_purchase"]).dt.days
    df["Frequency"] = df["invoice_count"]
    df["Monetary"] = df["monetary"]
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["Recency", "Frequency", "Monetary"])
    return df.reset_index(drop=True)


def plot_k_selection(scores, best_k):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
    ax1.plot(scores["k"], scores["inertia"], marker="o")
    ax1.set_title("Phương pháp Elbow")
    ax1.set_xlabel("Số cụm k")
    ax1.set_ylabel("Inertia")
    ax2.plot(scores["k"], scores["silhouette"], marker="o", color="tab:orange")
    ax2.axvline(best_k, color="gray", linestyle="--")
    ax2.set_title(f"Silhouette Score (chọn k = {best_k})")
    ax2.set_xlabel("Số cụm k")
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "kmeans_k_selection.png", dpi=150)
    plt.close(fig)


def plot_clusters(df, summary):
    fig, ax = plt.subplots(figsize=(9, 6))
    for name, group in df.groupby("segment"):
        ax.scatter(group["Recency"], group["Monetary"], s=8, alpha=0.5, label=f"{name} ({len(group)})")
    ax.set_yscale("log")
    ax.set_xlabel("Recency (ngày kể từ lần mua cuối)")
    ax.set_ylabel("Monetary (£, thang log)")
    ax.set_title("Phân nhóm khách hàng theo RFM")
    ax.legend(markerscale=3)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "customer_clusters.png", dpi=150)
    plt.close(fig)

    fig, axes = plt.subplots(1, 4, figsize=(15, 4))
    data = summary.set_index("segment")
    for ax, col, title in zip(axes, ["customers", "Recency", "Frequency", "Monetary"],
                              ["Số khách hàng", "Recency TB (ngày)", "Frequency TB (hóa đơn)", "Monetary TB (£)"]):
        ax.barh(data.index, data[col], color="tab:blue")
        ax.set_title(title)
        if ax is not axes[0]:
            ax.set_yticklabels([])
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "cluster_profiles.png", dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="RFM + K-Means")
    parser.add_argument("--k", type=int, default=None, help="Số cụm; bỏ trống = tự chọn theo Silhouette")
    args, _ = parser.parse_known_args()

    df = load_rfm()
    if len(df) < 10:
        raise ValueError("Cần ít nhất 10 khách hàng hợp lệ để chạy K-Means.")
    X = build_features(df)

    scores = evaluate_k(X)
    scores.round(4).to_csv(TABLES_DIR / "kmeans_k_selection.csv", index=False, encoding="utf-8-sig")
    best_k = args.k or int(scores.loc[scores["silhouette"].idxmax(), "k"])
    plot_k_selection(scores, best_k)

    model = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    df["cluster"] = model.fit_predict(X)
    score = float(silhouette_score(X, df["cluster"]))

    summary = df.groupby("cluster")[["Recency", "Frequency", "Monetary"]].mean()
    names = name_clusters(summary)
    df["segment"] = df["cluster"].map(names)
    summary["segment"] = summary.index.map(names)
    summary["customers"] = df["cluster"].value_counts()
    summary["revenue_share_pct"] = df.groupby("cluster")["Monetary"].sum() / df["Monetary"].sum() * 100
    summary["action"] = summary["segment"].map(ACTIONS).fillna("")
    summary = summary.reset_index().round(2)

    df.to_csv(TABLES_DIR / "customer_segments.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(TABLES_DIR / "cluster_summary.csv", index=False, encoding="utf-8-sig")
    plot_clusters(df, summary)

    print(scores.round(4).to_string(index=False))
    print(f"\nChọn k = {best_k} | Silhouette = {score:.4f}")
    print(summary.drop(columns="action").to_string(index=False))
    print(f"\nĐã lưu bảng trong {TABLES_DIR} và biểu đồ trong {CHARTS_DIR}")


if __name__ == "__main__":
    main()
