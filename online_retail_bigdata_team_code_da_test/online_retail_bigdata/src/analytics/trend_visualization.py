import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from src.common import TABLES_DIR, CHARTS_DIR, WEEKDAYS, find_beam_output

def load_trend(prefix, name):
    df = pd.read_csv(find_beam_output(prefix), skipinitialspace=True, dtype={name: str})
    df = df.iloc[:, :3]
    df.columns = [name, "revenue", "quantity"]
    return df

def bar_chart(df, label_col, title, filename, xlabel):
    plt.figure(figsize=(10, 5))
    plt.barh(df[label_col].astype(str)[::-1], df["revenue"][::-1])
    plt.title(title)
    plt.xlabel("Doanh thu (£)")
    plt.ylabel(xlabel)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / filename, dpi=150)
    plt.close()

def product_names():
    """Lấy mô tả đầu tiên của mỗi StockCode để biểu đồ dễ đọc hơn mã sản phẩm."""
    clean = pd.read_csv(find_beam_output("beam_clean_transactions"),
                        usecols=["StockCode", "Description"], dtype=str)
    return clean.dropna().drop_duplicates("StockCode").set_index("StockCode")["Description"]

def main():
    monthly = load_trend("month_trends", "month").sort_values("month")
    monthly.to_csv(TABLES_DIR / "monthly_trends.csv", index=False, encoding="utf-8-sig")
    plt.figure(figsize=(10, 5))
    plt.plot(monthly["month"], monthly["revenue"], marker="o")
    plt.title("Doanh thu theo tháng")
    plt.xlabel("Tháng")
    plt.ylabel("Doanh thu (£)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "monthly_revenue.png", dpi=150)
    plt.close()

    product = load_trend("product_trends", "product").sort_values("revenue", ascending=False).head(10)
    product["description"] = product["product"].map(product_names()).fillna(product["product"])
    product.to_csv(TABLES_DIR / "top_products.csv", index=False, encoding="utf-8-sig")
    bar_chart(product, "description", "Top 10 sản phẩm theo doanh thu", "top_products.png", "Sản phẩm")

    country = load_trend("country_trends", "country").sort_values("revenue", ascending=False).head(10)
    country.to_csv(TABLES_DIR / "top_countries.csv", index=False, encoding="utf-8-sig")
    bar_chart(country, "country", "Top 10 quốc gia theo doanh thu", "top_countries.png", "Quốc gia")

    category = load_trend("category_trends", "category").sort_values("revenue", ascending=False)
    category["revenue_share_pct"] = (category["revenue"] / category["revenue"].sum() * 100).round(2)
    category.to_csv(TABLES_DIR / "category_trends.csv", index=False, encoding="utf-8-sig")
    bar_chart(category, "category", "Doanh thu theo danh mục (suy ra từ Description)",
              "category_revenue.png", "Danh mục")

    wh = load_trend("weekday_hour_trends", "weekday_hour")
    wh[["weekday", "hour"]] = wh["weekday_hour"].str.split("-", expand=True).astype(int)
    wh["weekday_name"] = wh["weekday"].map(dict(enumerate(WEEKDAYS)))
    wh.sort_values(["weekday", "hour"]).drop(columns="weekday_hour").to_csv(
        TABLES_DIR / "weekday_hour_trends.csv", index=False, encoding="utf-8-sig")
    grid = wh.pivot_table(index="weekday", columns="hour", values="revenue", aggfunc="sum")
    grid = grid.reindex(index=range(7), columns=range(grid.columns.min(), grid.columns.max() + 1))
    plt.figure(figsize=(12, 4))
    plt.imshow(np.nan_to_num(grid.values), aspect="auto", cmap="Blues")
    plt.yticks(range(7), WEEKDAYS)
    plt.xticks(range(len(grid.columns)), [f"{h}h" for h in grid.columns])
    plt.colorbar(label="Doanh thu (£)")
    plt.title("Doanh thu theo thứ trong tuần và giờ trong ngày")
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "weekday_hour_heatmap.png", dpi=150)
    plt.close()

    by_day = wh.groupby("weekday_name")["revenue"].sum().reindex(WEEKDAYS).fillna(0)
    by_hour = wh.groupby("hour")["revenue"].sum()
    print("Doanh thu theo thứ:")
    print(by_day.round(0).to_string())
    print(f"Giờ cao điểm: {by_hour.idxmax()}h")
    print("Đã lưu bảng xu hướng trong results/tables và biểu đồ trong results/charts.")

if __name__ == "__main__":
    main()
