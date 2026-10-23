"""Web trình chiếu. Chạy: streamlit run app.py hoặc F5 trong Spyder"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import altair as alt
import pandas as pd
import streamlit as st
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from src.common import PROCESSED_DIR, TABLES_DIR, CHARTS_DIR, WEEKDAYS, run_as_streamlit
from src.analytics.customer_segmentation import ACTIONS, build_features, name_clusters

run_as_streamlit(__file__)
st.set_page_config(page_title="Online Retail Big Data", page_icon="🛒", layout="wide")


# ---------- Đọc dữ liệu ----------
def read_table(name):
    path = TABLES_DIR / name
    return pd.read_csv(path, encoding="utf-8-sig") if path.exists() else None


@st.cache_data
def load_rfm():
    df = read_table("customer_segments.csv")
    if df is None:
        return None
    df = df.dropna(subset=["Recency", "Frequency", "Monetary"]).reset_index(drop=True)
    return df, build_features(df)


def money(v):
    return f"£{v:,.0f}"


def missing(script):
    st.warning(f"Chưa có kết quả. Hãy chạy `python run_all.py` hoặc `{script}`.")


# ---------- Sidebar ----------
st.sidebar.title("🛒 Online Retail Big Data")
page = st.sidebar.radio("Nội dung", [
    "1. Giới thiệu đề tài",
    "2. Kiến trúc & công nghệ",
    "3. Dữ liệu & chất lượng",
    "4. Pipeline Apache Beam",
    "5. Xu hướng mua sắm",
    "6. Phân nhóm khách hàng (K-Means)",
    "7. Hiệu năng & khả năng mở rộng",
    "8. Kết luận & hướng phát triển",
])

quality = read_table("data_quality_summary.csv")
q = dict(zip(quality["metric"], quality["value"])) if quality is not None else {}

# ---------- 1. Giới thiệu ----------
if page.startswith("1"):
    st.title("Xây dựng hệ thống phân nhóm khách hàng và phân tích xu hướng mua sắm")
    st.subheader("từ dữ liệu giao dịch thương mại điện tử bằng Apache Beam")
    c = st.columns(4)
    c[0].metric("Giao dịch gốc", f"{int(q.get('rows_before', 0)):,}")
    c[1].metric("Giao dịch hợp lệ", f"{int(q.get('rows_after_sales_filter', 0)):,}")
    seg = read_table("customer_segments.csv")
    c[2].metric("Khách hàng", f"{len(seg):,}" if seg is not None else "—")
    countries = read_table("data_profile.csv")
    n_country = int(countries.set_index("column").loc["Country", "unique_values"]) if countries is not None else "—"
    c[3].metric("Quốc gia", n_country)

    st.markdown("""
### Bài toán
Một doanh nghiệp bán lẻ trực tuyến (UK, quà tặng & đồ trang trí) có hơn **540 nghìn dòng giao dịch** trong 13 tháng.
Doanh nghiệp cần biết:
- **Khách hàng của mình là ai?** → phân nhóm để chăm sóc và marketing đúng đối tượng.
- **Bán gì, bán khi nào, bán ở đâu?** → xu hướng theo sản phẩm, danh mục, thời gian, quốc gia.

### Tính ứng dụng thực tế
| Kết quả | Ứng dụng cho doanh nghiệp |
|---|---|
| Phân nhóm khách hàng (RFM + K-Means) | Marketing cá nhân hóa, giữ chân khách VIP, kéo lại khách rời bỏ |
| Xu hướng theo tháng / giờ / thứ | Lập kế hoạch tồn kho, nhân sự, thời điểm chạy khuyến mãi |
| Top sản phẩm / danh mục | Tối ưu danh mục hàng, nhập hàng trước mùa cao điểm |
| Doanh thu theo quốc gia | Ưu tiên mở rộng thị trường |

### Nguồn dữ liệu
**Online Retail Dataset** (UCI / Kaggle): giao dịch từ 01/12/2010 đến 09/12/2011 với 8 cột:
`InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country`.
""")

# ---------- 2. Kiến trúc ----------
elif page.startswith("2"):
    st.title("Kiến trúc hệ thống & mô hình xử lý")
    arch = CHARTS_DIR / "architecture.png"
    if arch.exists():
        st.image(str(arch), use_container_width=True)
    st.markdown("""
| Tầng | Công nghệ | Vai trò |
|---|---|---|
| Thu thập | Kaggle / UCI | Nguồn dữ liệu giao dịch |
| Lưu trữ phân tán | **HDFS** (namenode + datanode, Docker) · CSV/Parquet | Lưu dữ liệu thô và kết quả xử lý; Parquet nén dạng cột |
| Xử lý | **Apache Beam** (Python SDK) | Mô hình lập trình hợp nhất batch/stream, chạy trên nhiều runner: Direct, Flink, Spark, Dataflow |
| Học máy | scikit-learn: K-Means, StandardScaler, Silhouette | Phân nhóm khách hàng theo RFM |
| Trực quan | Matplotlib, Streamlit, Altair | Biểu đồ, web trình bày kết quả |

### Vì sao chọn Apache Beam?
- Viết **một lần**, chạy trên **nhiều engine phân tán** (Flink, Spark, Google Dataflow) chỉ bằng đổi tham số `--runner`.
- Mô hình **PCollection + PTransform** (Map, FlatMap, CombinePerKey) tự song song hóa theo khóa, tương tự MapReduce.
- Đọc/ghi trực tiếp `hdfs://` qua `--hdfs_host --hdfs_port`.

### Lưu trữ trên HDFS
```bash
docker compose -f docker/docker-compose.yml up -d          # dựng namenode + datanode
python -m src.data_prep.hdfs_tool put data/raw/customer.csv /retail/raw/customer.csv
python run_all.py --hdfs --workers 4                        # Beam đọc/ghi hdfs://retail/...
```
""")

# ---------- 3. Dữ liệu ----------
elif page.startswith("3"):
    st.title("Khảo sát dữ liệu & kiểm tra chất lượng")
    c = st.columns(5)
    c[0].metric("Tổng số dòng", f"{int(q.get('rows_before', 0)):,}")
    c[1].metric("Thiếu CustomerID", f"{int(q.get('missing_customer_id', 0)):,}")
    c[2].metric("Hóa đơn hủy (C...)", f"{int(q.get('cancelled_invoice_rows', 0)):,}")
    c[3].metric("Số lượng âm", f"{int(q.get('negative_quantity_rows', 0)):,}")
    c[4].metric("Dòng trùng lặp", f"{int(q.get('exact_duplicate_rows', 0)):,}")

    before = int(q.get("rows_before", 0))
    after = int(q.get("rows_after_sales_filter", 0))
    funnel = pd.DataFrame({
        "Bước": ["Dữ liệu gốc", "Sau khi lọc (dữ liệu bán hàng hợp lệ)"],
        "Số dòng": [before, after],
    })
    st.altair_chart(alt.Chart(funnel).mark_bar().encode(
        x="Số dòng:Q", y=alt.Y("Bước:N", sort=None), tooltip=["Bước", "Số dòng"]
    ).properties(height=150), use_container_width=True)
    st.caption(f"Giữ lại {after / before:.1%} số dòng. Pipeline Beam cho ra đúng {after:,} dòng, "
               "khớp với bản đối chứng bằng pandas." if before else "")

    st.subheader("Hồ sơ các cột")
    profile = read_table("data_profile.csv")
    if profile is not None:
        profile["% thiếu"] = (profile["missing"] / before * 100).round(2)
        st.dataframe(profile, use_container_width=True, hide_index=True)

    st.subheader("Quy tắc làm sạch")
    st.markdown("""
1. Bỏ hóa đơn hủy (`InvoiceNo` bắt đầu bằng **C**).
2. Bỏ dòng `Quantity ≤ 0` hoặc `UnitPrice ≤ 0`.
3. Bỏ dòng thiếu `CustomerID` (không thể phân nhóm khách hàng).
4. Tạo cột mới `line_revenue = Quantity × UnitPrice`.
5. Xuất **CSV** và **Parquet** (nén dạng cột, đọc nhanh hơn).
""")

# ---------- 4. Pipeline ----------
elif page.startswith("4"):
    st.title("Pipeline xử lý bằng Apache Beam")
    tab1, tab2, tab3 = st.tabs(["Pipeline làm sạch", "Pipeline khách hàng", "Pipeline xu hướng"])
    with tab1:
        st.graphviz_chart("""digraph{rankdir=LR;node[shape=box,style=rounded];
ReadFromText->"FlatMap(parse_and_clean)"->"Map(to_csv_line)"->WriteToText}""")
        st.code('''p | "Read CSV lines"   >> beam.io.ReadFromText(input, skip_header_lines=1)   # local hoặc hdfs://
  | "Parse and filter" >> beam.FlatMap(parse_and_clean)   # lọc hóa đơn hủy, SL/giá <= 0, thiếu KH
  | "Encode CSV line"  >> beam.Map(...)
  | "Write cleaned"    >> beam.io.WriteToText(output, file_name_suffix=".csv")''', language="python")
    with tab2:
        st.graphviz_chart("""digraph{rankdir=LR;node[shape=box,style=rounded];
ReadFromText->"FlatMap(parse_customer)\\n(CustomerID, (date, invoice, qty, revenue))"->"CombinePerKey(CustomerCombineFn)\\nlast_purchase, invoice_count, monetary"->WriteToText}""")
        st.markdown("`CustomerCombineFn` tổng hợp cục bộ trên từng worker rồi mới trộn kết quả "
                    "(**combiner lifting**), nên lượng dữ liệu phải shuffle giữa các worker ít hơn `GroupByKey`. "
                    "Kết quả là bảng **đặc trưng RFM** cho từng khách hàng, đầu vào của K-Means.")
    with tab3:
        st.graphviz_chart("""digraph{rankdir=LR;node[shape=box,style=rounded];
ReadFromText->"FlatMap(parse_row)";
"FlatMap(parse_row)"->"Sum month"->month_trends;
"FlatMap(parse_row)"->"Sum product"->product_trends;
"FlatMap(parse_row)"->"Sum country"->country_trends;
"FlatMap(parse_row)"->"Sum category"->category_trends;
"FlatMap(parse_row)"->"Sum weekday_hour"->weekday_hour_trends}""")
        st.markdown("Đọc dữ liệu **một lần**, rẽ thành 5 nhánh `CombinePerKey` chạy song song.")

    st.subheader("Kết quả đầu ra của Beam")
    outputs = []
    for f in sorted(PROCESSED_DIR.glob("*-00000-of-*.csv")):
        outputs.append({"File": f.name, "Kích thước (MB)": round(f.stat().st_size / 1024 ** 2, 2)})
    st.dataframe(pd.DataFrame(outputs), use_container_width=True, hide_index=True)
    st.markdown("""
**Chọn cách chạy bằng tham số, không cần sửa code:**
```bash
python -m src.beam_pipelines.pipeline_clean                                                   # 1 worker
python -m src.beam_pipelines.pipeline_clean --direct_num_workers=4 --direct_running_mode=multi_processing
python -m src.beam_pipelines.pipeline_clean --runner=FlinkRunner --flink_master=localhost:8081   # cụm Flink
```""")

# ---------- 5. Xu hướng ----------
elif page.startswith("5"):
    st.title("Phân tích xu hướng mua sắm")
    monthly = read_table("monthly_trends.csv")
    if monthly is None:
        missing("python -m src.analytics.trend_visualization")
        st.stop()
    st.subheader("Doanh thu theo tháng")
    st.altair_chart(alt.Chart(monthly).mark_line(point=True).encode(
        x=alt.X("month:N", title="Tháng"),
        y=alt.Y("revenue:Q", title="Doanh thu (£)"),
        tooltip=["month", alt.Tooltip("revenue:Q", format=",.0f"), alt.Tooltip("quantity:Q", format=",.0f")],
    ).properties(height=320), use_container_width=True)
    peak = monthly.loc[monthly["revenue"].idxmax()]
    st.caption(f"Đỉnh doanh thu: **{peak['month']}** ({money(peak['revenue'])}). "
               "Doanh thu tăng mạnh từ tháng 9 đến tháng 11 do mùa mua sắm cuối năm. "
               "Tháng 12/2011 chỉ có dữ liệu đến ngày 09.")

    col1, col2 = st.columns(2)
    countries = read_table("top_countries.csv")
    if countries is not None:
        with col1:
            st.subheader("Top 10 quốc gia")
            exclude_uk = st.checkbox("Bỏ United Kingdom để dễ so sánh")
            data = countries[countries["country"] != "United Kingdom"] if exclude_uk else countries
            st.altair_chart(alt.Chart(data).mark_bar().encode(
                x=alt.X("revenue:Q", title="Doanh thu (£)"),
                y=alt.Y("country:N", sort="-x", title=None),
                tooltip=["country", alt.Tooltip("revenue:Q", format=",.0f")],
            ), use_container_width=True)
    products = read_table("top_products.csv")
    if products is not None:
        with col2:
            st.subheader("Top 10 sản phẩm")
            st.altair_chart(alt.Chart(products).mark_bar().encode(
                x=alt.X("revenue:Q", title="Doanh thu (£)"),
                y=alt.Y("description:N", sort="-x", title=None),
                tooltip=["product", "description", alt.Tooltip("revenue:Q", format=",.0f")],
            ), use_container_width=True)

    category = read_table("category_trends.csv")
    if category is not None:
        st.subheader("Doanh thu theo danh mục sản phẩm")
        st.caption("Danh mục được Beam gán theo từ khóa trong cột Description "
                   "(bộ dữ liệu gốc không có cột danh mục).")
        st.altair_chart(alt.Chart(category).mark_bar().encode(
            x=alt.X("revenue:Q", title="Doanh thu (£)"),
            y=alt.Y("category:N", sort="-x", title=None),
            tooltip=["category", alt.Tooltip("revenue:Q", format=",.0f"), "revenue_share_pct"],
        ), use_container_width=True)

    heat = read_table("weekday_hour_trends.csv")
    if heat is not None:
        st.subheader("Thời điểm mua sắm: thứ trong tuần × giờ")
        st.altair_chart(alt.Chart(heat).mark_rect().encode(
            x=alt.X("hour:O", title="Giờ"), y=alt.Y("weekday_name:N", sort=WEEKDAYS, title=None),
            color=alt.Color("revenue:Q", title="Doanh thu", scale=alt.Scale(scheme="blues")),
            tooltip=["weekday_name", "hour", alt.Tooltip("revenue:Q", format=",.0f")],
        ).properties(height=260), use_container_width=True)
        by_day = heat.groupby("weekday_name")["revenue"].sum()
        by_hour = heat.groupby("hour")["revenue"].sum()
        absent = [d for d in WEEKDAYS if d not in by_day.index]
        st.caption(f"Ngày bán nhiều nhất: **{by_day.idxmax()}**. Giờ cao điểm: **{by_hour.idxmax()}h**. "
                   + (f"Không có giao dịch vào {', '.join(absent)}. " if absent else "")
                   + "Khách chủ yếu mua trong giờ hành chính, phù hợp với tệp khách sỉ.")

# ---------- 6. K-Means ----------
elif page.startswith("6"):
    st.title("Phân nhóm khách hàng: RFM + K-Means")
    st.markdown("""
**Đặc trưng RFM** (tính bằng Beam pipeline 2):
- **Recency**: số ngày kể từ lần mua gần nhất (càng nhỏ càng tốt)
- **Frequency**: số hóa đơn
- **Monetary**: tổng tiền đã chi

Tiền xử lý: `log1p` cho F, M (giảm lệch phải) → `StandardScaler` → **K-Means**.
""")
    loaded = load_rfm()
    scores = read_table("kmeans_k_selection.csv")
    if loaded is None or scores is None:
        missing("python -m src.analytics.customer_segmentation")
        st.stop()
    rfm, X = loaded
    rfm = rfm.copy()
    best_k = int(scores.loc[scores["silhouette"].idxmax(), "k"])

    st.subheader("Chọn số cụm k")
    c1, c2 = st.columns(2)
    c1.altair_chart(alt.Chart(scores).mark_line(point=True).encode(
        x="k:O", y=alt.Y("inertia:Q", title="Inertia")
    ).properties(title="Phương pháp Elbow", height=250), use_container_width=True)
    c2.altair_chart(alt.Chart(scores).mark_line(point=True, color="orange").encode(
        x="k:O", y=alt.Y("silhouette:Q", title="Silhouette", scale=alt.Scale(zero=False))
    ).properties(title=f"Silhouette Score (cao nhất tại k = {best_k})", height=250), use_container_width=True)

    k = st.slider("Số cụm k (mặc định = k có Silhouette cao nhất)", 2, 8, best_k)
    model = KMeans(n_clusters=k, random_state=42, n_init=10).fit(X)
    rfm["cluster"] = model.labels_
    summary = rfm.groupby("cluster")[["Recency", "Frequency", "Monetary"]].mean()
    summary["Số KH"] = rfm["cluster"].value_counts()
    summary["% doanh thu"] = rfm.groupby("cluster")["Monetary"].sum() / rfm["Monetary"].sum() * 100
    names = name_clusters(summary)
    summary["Nhóm"] = summary.index.map(names)
    rfm["Nhóm"] = rfm["cluster"].map(names)
    st.metric("Silhouette với k đã chọn", f"{silhouette_score(X, model.labels_):.3f}")

    st.subheader("Đặc điểm từng nhóm")
    show = summary[["Nhóm", "Số KH", "Recency", "Frequency", "Monetary", "% doanh thu"]].round(1)
    show["Gợi ý hành động"] = show["Nhóm"].map(ACTIONS).fillna("")
    st.dataframe(show, use_container_width=True, hide_index=True)

    c1, c2 = st.columns([3, 2])
    c1.altair_chart(alt.Chart(rfm).mark_circle(opacity=0.6, size=30).encode(
        x=alt.X("Recency:Q", title="Recency (ngày)"),
        y=alt.Y("Monetary:Q", scale=alt.Scale(type="log"), title="Monetary (£, thang log)"),
        color=alt.Color("Nhóm:N"),
        tooltip=["CustomerID", "Nhóm", "Recency", "Frequency", alt.Tooltip("Monetary:Q", format=",.0f")],
    ).properties(title="Recency × Monetary", height=380).interactive(), use_container_width=True)
    c2.altair_chart(alt.Chart(summary.reset_index()).mark_arc(innerRadius=50).encode(
        theta="Số KH:Q", color="Nhóm:N", tooltip=["Nhóm", "Số KH"]
    ).properties(title="Tỷ lệ khách hàng theo nhóm", height=380), use_container_width=True)

    st.subheader("Tra cứu một khách hàng")
    cid = st.selectbox("CustomerID", rfm["CustomerID"].astype(str).sort_values().unique())
    row = rfm[rfm["CustomerID"].astype(str) == cid].iloc[0]
    c = st.columns(4)
    c[0].metric("Nhóm", row["Nhóm"])
    c[1].metric("Recency", f"{int(row['Recency'])} ngày")
    c[2].metric("Frequency", f"{int(row['Frequency'])} hóa đơn")
    c[3].metric("Monetary", money(row["Monetary"]))
    st.caption("Gợi ý: " + ACTIONS.get(row["Nhóm"], ""))

# ---------- 7. Hiệu năng ----------
elif page.startswith("7"):
    st.title("Hiệu năng & khả năng mở rộng")
    bench = read_table("benchmark.csv")
    if bench is None or "method" not in bench.columns:
        missing("python -m src.analytics.benchmark")
    else:
        st.markdown("Cùng một phép làm sạch dữ liệu, đo thời gian khi dữ liệu tăng dần "
                    "(x5 = nhân bản file gốc 5 lần, **dữ liệu tổng hợp để thử tải**).")
        bench["Triệu dòng"] = (bench["input_rows"] / 1e6).round(2)
        st.altair_chart(alt.Chart(bench).mark_line(point=True).encode(
            x=alt.X("Triệu dòng:Q"), y=alt.Y("elapsed_seconds:Q", title="Thời gian (giây)"),
            color=alt.Color("method:N", title="Cách xử lý"),
            tooltip=["method", "scale", "input_rows", "input_mb", "elapsed_seconds", "rows_per_second"],
        ).properties(height=320), use_container_width=True)
        st.dataframe(bench.drop(columns="Triệu dòng"), use_container_width=True, hide_index=True)
        largest = bench[bench["scale"] == bench["scale"].max()].set_index("method")["elapsed_seconds"]
        beam = largest[largest.index.str.startswith("Beam")]
        if len(beam) == 2:
            st.caption(f"Ở mức lớn nhất, Beam nhiều worker nhanh hơn Beam 1 worker "
                       f"**{beam.iloc[0] / beam.iloc[1]:.1f} lần**.")
    st.markdown("""
### Dữ liệu này có phải Big Data không?
Với ~0,5 triệu dòng / 46 MB, **một máy vẫn xử lý được**, và pandas còn nhanh hơn Beam vì Beam tốn chi phí
khởi tạo worker và tuần tự hóa dữ liệu. Lợi thế của Beam là **mở rộng theo chiều ngang**:

| Đặc điểm | Hiện tại | Khi dữ liệu lớn (hàng trăm triệu dòng) |
|---|---|---|
| **Volume** | 46 MB (x10 ≈ 460 MB, 5,4 triệu dòng khi thử tải) | HDFS chia file thành block 128 MB, nhân bản trên nhiều datanode |
| **Velocity** | Batch theo file | Beam hỗ trợ streaming (Kafka / PubSub) cùng một code |
| **Variety** | CSV có cấu trúc | Đọc CSV, Parquet, JSON qua `beam.io` |
| **Xử lý** | DirectRunner nhiều tiến trình (1 máy) | Flink / Spark / Dataflow nhiều máy, không cần sửa logic |

pandas phải nạp toàn bộ dữ liệu vào RAM của một máy; Beam xử lý theo từng phần tử và chia việc cho nhiều worker.
""")

# ---------- 8. Kết luận ----------
else:
    st.title("Kết luận & hướng phát triển")
    summary = read_table("cluster_summary.csv")
    segments = ""
    if summary is not None:
        segments = ", ".join(f"{r.segment} ({int(r.customers):,} KH)" for r in summary.itertuples())
    st.markdown(f"""
### Kết quả đạt được
- Xây dựng **3 pipeline Apache Beam** làm sạch, biến đổi và tổng hợp dữ liệu; chạy được song song nhiều worker.
- Lọc từ **{int(q.get('rows_before', 0)):,}** xuống **{int(q.get('rows_after_sales_filter', 0)):,}** giao dịch hợp lệ.
- Phân nhóm khách hàng bằng **K-Means**, chọn k theo Elbow + Silhouette: {segments}.
- Phân tích xu hướng theo tháng, sản phẩm, danh mục, quốc gia, thứ trong tuần và giờ.

### Hạn chế
- Dữ liệu gốc còn nhỏ; phần thử tải dùng dữ liệu nhân bản.
- Cụm HDFS chỉ có 1 namenode + 1 datanode trên cùng một máy; Beam chạy DirectRunner nhiều tiến trình,
  chưa triển khai trên cụm Flink/Spark nhiều máy.
- Danh mục sản phẩm suy ra bằng từ khóa nên chưa chính xác tuyệt đối.

### Hướng phát triển
1. Chạy Beam trên **Flink/Spark runner** nhiều máy, HDFS nhiều datanode.
2. Thêm luồng **streaming** (Kafka → Beam) để cập nhật phân nhóm gần thời gian thực.
3. Thử DBSCAN, GMM và dự báo doanh thu theo chuỗi thời gian.
""")
