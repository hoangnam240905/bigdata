"""Vẽ sơ đồ kiến trúc hệ thống ra results/charts/architecture.png (dùng cho báo cáo)."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from src.common import CHARTS_DIR

# (x, y, nhãn, màu) - tọa độ tâm hộp
BOXES = {
    "src": (1.5, 3, "Kaggle / UCI\nOnline Retail CSV\n541.909 dòng", "#dbe7f7"),
    "hdfs": (4.7, 3, "Lưu trữ\nHDFS (namenode +\ndatanode) / local\nCSV + Parquet", "#fde9c8"),
    "clean": (7.9, 3, "Beam pipeline 1\nLàm sạch & biến đổi\n(FlatMap)", "#d6f0dc"),
    "cust": (11.1, 4.4, "Beam pipeline 2\nTổng hợp RFM\n(CombinePerKey)", "#d6f0dc"),
    "trend": (11.1, 1.6, "Beam pipeline 3\nXu hướng tháng / SP /\ndanh mục / giờ", "#d6f0dc"),
    "ml": (14.3, 4.4, "Học máy\nRFM + K-Means\n(scikit-learn)", "#eadcf5"),
    "viz": (17.5, 3, "Trực quan hóa\nMatplotlib +\nWeb Streamlit", "#f8d9df"),
}
EDGES = [("src", "hdfs"), ("hdfs", "clean"), ("clean", "cust"), ("clean", "trend"),
         ("cust", "ml"), ("ml", "viz"), ("trend", "viz")]
W, H = 2.7, 1.25


def main():
    fig, ax = plt.subplots(figsize=(18, 6.5))
    ax.set_xlim(0, 19)
    ax.set_ylim(0, 6.2)
    ax.axis("off")
    for x, y, label, color in BOXES.values():
        ax.add_patch(FancyBboxPatch((x - W / 2, y - H / 2), W, H, boxstyle="round,pad=0.08",
                                    facecolor=color, edgecolor="#555"))
        ax.text(x, y, label, ha="center", va="center", fontsize=10)
    for a, b in EDGES:
        xa, ya = BOXES[a][:2]
        xb, yb = BOXES[b][:2]
        start, end = (xa + W / 2 + 0.08, ya), (xb - W / 2 - 0.08, yb)
        ax.annotate("", xy=end, xytext=start, arrowprops=dict(arrowstyle="->", lw=1.6, color="#333"))
    ax.text(9.5, 5.75, "Apache Beam (DirectRunner / Flink / Spark)", ha="center", fontsize=11, weight="bold")
    for x, text in [(1.5, "Thu thập"), (4.7, "Lưu trữ"), (9.5, "Xử lý phân tán"), (14.3, "Phân tích"), (17.5, "Trình bày")]:
        ax.text(x, 0.15, text, ha="center", fontsize=11, color="#444", style="italic")
    ax.set_title("Kiến trúc hệ thống phân nhóm khách hàng & phân tích xu hướng mua sắm", fontsize=14)
    out = CHARTS_DIR / "architecture.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Đã lưu sơ đồ kiến trúc: {out}")


if __name__ == "__main__":
    main()
