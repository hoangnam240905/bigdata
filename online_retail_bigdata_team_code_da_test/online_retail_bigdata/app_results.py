"""Dashboard kết quả (đọc output của run_all.py). Chạy: streamlit run app_results.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st

from src.common import PROCESSED_DIR, TABLES_DIR, CHARTS_DIR, WEEKDAYS

st.set_page_config(page_title="Kết quả Online Retail", page_icon="📊", layout="wide")

NO_RESULT = "Chưa có kết quả, hãy chạy `python run_all.py`."

QUALITY_LABELS = {
    "source_file": "File nguồn",
    "rows_before": "Dòng thô ban đầu",
    "rows_after_sales_filter": "Dòng sau lọc bán hàng",
    "invalid_dates": "Ngày không hợp lệ",
    "invalid_quantity_or_price": "Số lượng/giá không hợp lệ",
    "missing_customer_id": "Thiếu mã khách",
    "exact_duplicate_rows": "Dòng trùng hoàn toàn",
    "negative_quantity_rows": "Dòng số lượng âm",
    "cancelled_invoice_rows": "Dòng thuộc hóa đơn hủy",
    "parquet_status": "Trạng thái ghi Parquet",
}


# ---------- Đọc dữ liệu: file thiếu -> None ----------
def table_path(name):
    path = TABLES_DIR / name
    return path if path.exists() else None


def beam_path(prefix):
    files = sorted(PROCESSED_DIR.glob(prefix + "-*.csv"))
    return files[0] if files else None


@st.cache_data
def read_csv(path, **kwargs):
    if path is None:
        return None
    return pd.read_csv(path, encoding="utf-8-sig", **kwargs)


def read_table(name):
    return read_csv(table_path(name))


def read_beam(prefix, key):
    return read_csv(beam_path(prefix), dtype={key: str})


@st.cache_data
def count_rows(path):
    if path is None:
        return None
    return len(pd.read_csv(path, usecols=[0], encoding="utf-8-sig"))


def image(name, caption=None):
    path = CHARTS_DIR / name
    if path.exists():
        st.image(str(path), caption=caption, use_container_width=True)
    else:
        st.warning(f"Chưa có ảnh {name}. {NO_RESULT}")


def money(v):
    return f"£{v:,.0f}"


def fmt_int(v):
    return f"{int(v):,}"


quality = read_table("data_quality_summary.csv")
q = dict(zip(quality["metric"], quality["value"])) if quality is not None else {}
segments = read_table("customer_segments.csv")
k_scores = read_table("kmeans_k_selection.csv")

# ---------- Sidebar: tải CSV kết quả ----------
st.sidebar.title("📊 Kết quả Online Retail")
st.sidebar.caption("Mọi số liệu đọc trực tiếp từ output của `run_all.py`.")
st.sidebar.subheader("Tải CSV kết quả")
for name in ["data_quality_summary.csv", "data_profile.csv", "customer_segments.csv", "cluster_summary.csv",
             "kmeans_k_selection.csv", "monthly_trends.csv", "category_trends.csv", "benchmark.csv"]:
    path = table_path(name)
    if path is not None:
        st.sidebar.download_button(name, path.read_bytes(), file_name=name, mime="text/csv",
                                   use_container_width=True)

st.title("Kết quả phân tích dữ liệu Online Retail")
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Tổng quan số liệu", "Chất lượng dữ liệu & Beam", "Phân nhóm khách hàng", "Xu hướng mua sắm", "Hiệu năng",
])

# ---------- Tab 1: Tổng quan ----------
with tab1:
    if quality is None or segments is None or k_scores is None:
        st.warning(NO_RESULT)
    else:
        before = int(q["rows_before"])
        after = int(q["rows_after_sales_filter"])
        best = k_scores.loc[k_scores["silhouette"].idxmax()]
        c = st.columns(4)
        c[0].metric("Dòng thô ban đầu", fmt_int(before))
        c[1].metric("Dòng sau làm sạch", fmt_int(after))
        c[2].metric("Dòng bị loại", fmt_int(before - after), f"-{(before - after) / before:.1%}",
                    delta_color="off")
        c[3].metric("Số khách hàng", fmt_int(len(segments)))
        c = st.columns(4)
        c[0].metric("Tổng doanh thu (khách có mã)", money(segments["Monetary"].sum()))
        c[1].metric("Số nhóm K tối ưu", int(best["k"]))
        c[2].metric("Silhouette score", f"{best['silhouette']:.4f}")
        profile = read_table("data_profile.csv")
        if profile is not None:
            c[3].metric("Số quốc gia", fmt_int(profile.set_index("column").loc["Country", "unique_values"]))

    st.subheader("Khảo sát dữ liệu thô")
    profile = read_table("data_profile.csv")
    if profile is None:
        st.warning(NO_RESULT)
    else:
        st.dataframe(profile.rename(columns={
            "column": "Cột", "dtype": "Kiểu dữ liệu", "non_null": "Không thiếu",
            "missing": "Thiếu", "unique_values": "Giá trị duy nhất"}), use_container_width=True, hide_index=True)

# ---------- Tab 2: Chất lượng & Beam ----------
with tab2:
    st.subheader("Chất lượng dữ liệu")
    if quality is None:
        st.warning(NO_RESULT)
    else:
        shown = quality[quality["metric"] != "source_file"].copy()
        shown["metric"] = shown["metric"].map(QUALITY_LABELS).fillna(shown["metric"])
        st.dataframe(shown.rename(columns={"metric": "Chỉ số", "value": "Giá trị"}),
                     use_container_width=True, hide_index=True)

    st.subheader("Đối chứng hai bản làm sạch")
    pandas_file = PROCESSED_DIR / "clean_transactions.csv"
    beam_file = beam_path("beam_clean_transactions")
    n_pandas = count_rows(pandas_file if pandas_file.exists() else None)
    n_beam = count_rows(beam_file)
    if n_pandas is None or n_beam is None:
        st.warning(NO_RESULT)
    else:
        c = st.columns(3)
        c[0].metric("Bản pandas (clean_transactions.csv)", fmt_int(n_pandas))
        c[1].metric(f"Bản Beam ({beam_file.name})", fmt_int(n_beam))
        c[2].metric("Kết quả", "Khớp ✅" if n_pandas == n_beam else f"Lệch {abs(n_pandas - n_beam):,} dòng ❌")
    st.caption("Danh mục sản phẩm được suy ra từ từ khóa trong Description (CATEGORY_RULES trong "
               "src/common.py), không phải danh mục gốc của bộ dữ liệu.")

# ---------- Tab 3: K-Means ----------
with tab3:
    summary = read_table("cluster_summary.csv")
    if k_scores is None or summary is None or segments is None:
        st.warning(NO_RESULT)
    else:
        st.subheader("Chọn số nhóm K")
        c1, c2 = st.columns([1, 2])
        c1.dataframe(k_scores.rename(columns={"inertia": "Inertia (Elbow)", "silhouette": "Silhouette"}),
                     use_container_width=True, hide_index=True)
        with c2:
            image("kmeans_k_selection.png")

        st.subheader("Hồ sơ từng nhóm khách hàng")
        st.dataframe(summary[["segment", "customers", "Recency", "Frequency", "Monetary",
                              "revenue_share_pct", "action"]].rename(columns={
            "segment": "Nhóm", "customers": "Số khách", "Recency": "Recency TB (ngày)",
            "Frequency": "Frequency TB (hóa đơn)", "Monetary": "Monetary TB (£)",
            "revenue_share_pct": "% doanh thu", "action": "Hành động đề xuất"}),
            use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            image("customer_clusters.png", "Recency × Monetary theo nhóm")
        with c2:
            image("cluster_profiles.png", "Hồ sơ trung bình của từng nhóm")

        st.subheader("Tra cứu khách hàng")
        cid = st.text_input("Nhập CustomerID (ví dụ 12583)").strip()
        if cid:
            key = cid[:-2] if cid.endswith(".0") else cid
            row = segments[segments["CustomerID"].astype(str) == key]
            if row.empty:
                st.error(f"Không tìm thấy khách hàng {cid}.")
            else:
                r = row.iloc[0]
                actions = dict(zip(summary["segment"], summary["action"]))
                c = st.columns(4)
                c[0].metric("Nhóm", r["segment"])
                c[1].metric("Recency", f"{int(r['Recency'])} ngày")
                c[2].metric("Frequency", f"{int(r['Frequency'])} hóa đơn")
                c[3].metric("Monetary", money(r["Monetary"]))
                st.info("Hành động đề xuất: " + str(actions.get(r["segment"], "")))

# ---------- Tab 4: Xu hướng ----------
with tab4:
    month = read_beam("month_trends", "month")
    st.subheader("Doanh thu theo tháng")
    if month is None:
        st.warning(NO_RESULT)
    else:
        month = month.sort_values("month").reset_index(drop=True)
        st.line_chart(month, x="month", y="revenue", x_label="Tháng", y_label="Doanh thu (£)")
        st.dataframe(month, use_container_width=True, hide_index=True)

    st.subheader("Top 10 sản phẩm theo doanh thu")
    product = read_beam("product_trends", "product")
    if product is None:
        st.warning(NO_RESULT)
    else:
        product = product.sort_values("revenue", ascending=False).head(10).reset_index(drop=True)
        names = read_table("top_products.csv")
        if names is not None and "description" in names.columns:
            product["description"] = product["product"].map(
                dict(zip(names["product"].astype(str), names["description"])))
        product.insert(0, "hạng", range(1, len(product) + 1))
        st.bar_chart(product, x="product", y="revenue", x_label="Mã sản phẩm", y_label="Doanh thu (£)",
                     sort="-revenue")
        st.dataframe(product, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Doanh thu theo danh mục")
        category = read_beam("category_trends", "category")
        if category is None:
            st.warning(NO_RESULT)
        else:
            category = category.sort_values("revenue", ascending=False).reset_index(drop=True)
            st.bar_chart(category, x="category", y="revenue", x_label="Danh mục", y_label="Doanh thu (£)",
                         sort="-revenue", horizontal=True)
            st.dataframe(category, use_container_width=True, hide_index=True)
    with col2:
        st.subheader("Top 10 quốc gia")
        country = read_beam("country_trends", "country")
        if country is None:
            st.warning(NO_RESULT)
        else:
            country = country.sort_values("revenue", ascending=False).head(10).reset_index(drop=True)
            st.bar_chart(country, x="country", y="revenue", x_label="Quốc gia", y_label="Doanh thu (£)",
                         sort="-revenue", horizontal=True)
            st.dataframe(country, use_container_width=True, hide_index=True)

    st.subheader("Doanh thu theo thứ × giờ (£)")
    wh = read_beam("weekday_hour_trends", "weekday_hour")
    if wh is None:
        st.warning(NO_RESULT)
    else:
        parts = wh["weekday_hour"].str.split("-", expand=True).astype(int)
        wh["Thứ"] = parts[0].map(dict(enumerate(WEEKDAYS)))
        wh["giờ"] = parts[1]
        grid = (wh.pivot_table(index="Thứ", columns="giờ", values="revenue", aggfunc="sum")
                .reindex(WEEKDAYS).fillna(0))
        grid.columns = [f"{h}h" for h in grid.columns]
        st.dataframe(grid.style.format("{:,.0f}").background_gradient(cmap="Blues", axis=None),
                     use_container_width=True)
        peak = wh.loc[wh["revenue"].idxmax()]
        st.caption(f"Khung giờ doanh thu cao nhất: {peak['Thứ']} lúc {peak['giờ']}h "
                   f"({money(peak['revenue'])}). Khóa 0-10 trong file nghĩa là Thứ 2 lúc 10h.")

# ---------- Tab 5: Hiệu năng ----------
with tab5:
    bench = read_table("benchmark.csv")
    st.subheader("Benchmark làm sạch dữ liệu: pandas vs Apache Beam")
    if bench is None:
        st.warning(NO_RESULT)
    else:
        st.dataframe(bench.rename(columns={
            "method": "Phương pháp", "scale": "Hệ số nhân", "input_rows": "Số dòng đầu vào",
            "input_mb": "Dung lượng (MB)", "elapsed_seconds": "Thời gian (giây)",
            "rows_per_second": "Tốc độ (dòng/giây)"}), use_container_width=True, hide_index=True)
    image("benchmark.png")
    st.caption("Lưu ý: dữ liệu x5, x10 là bản nhân bản file gốc để thử tải, "
               "không phải giao dịch kinh doanh thật.")
