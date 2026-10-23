# Online Retail Big Data — Apache Beam + Python

Chạy bằng Spyder hoặc terminal trên Windows.

## 1. Cấu trúc

- `src/data_prep/` — khảo sát dữ liệu, kiểm tra chất lượng, xuất Parquet, đưa dữ liệu lên HDFS (`hdfs_tool.py`).
- `src/beam_pipelines/` — 3 pipeline Apache Beam làm sạch / tổng hợp RFM / tổng hợp xu hướng.
- `src/analytics/` — RFM + K-Means, trực quan hóa, benchmark, sơ đồ kiến trúc.
- `src/common.py` — dùng chung: đường dẫn, quy tắc danh mục sản phẩm, cấu hình runner Beam.
- `docker/` — cụm HDFS (1 namenode + 1 datanode).
- `run_all.py` — chạy toàn bộ quy trình theo đúng thứ tự.
- `app_results.py` — web hiển thị kết quả và số liệu.
- `data/raw/` — đặt CSV tải từ Kaggle vào đây.
- `data/processed/` — dữ liệu đã xử lý.
- `results/` — bảng kết quả và biểu đồ.

## 2. Cài đặt

Khuyến nghị Python 3.11 trong môi trường riêng. Mở Anaconda Prompt tại thư mục dự án:

```bash
conda create -n bigdata python=3.11 -y
conda activate bigdata
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Nếu đã cài Spyder trong Anaconda:
```bash
conda install spyder -y
python -m spyder
```

Trong Spyder, chọn interpreter của môi trường `bigdata` trong Preferences > Python interpreter. Nếu Spyder nằm ở môi trường khác, cài `spyder-kernels` tương thích vào môi trường `bigdata`.

## 3. Chuẩn bị dữ liệu

Tải Online Retail Dataset từ Kaggle, giải nén và đặt file CSV vào `data/raw/`.
Mặc định chương trình tìm CSV đầu tiên trong thư mục này. Có thể truyền đường dẫn file khác bằng tham số `--input`.

Bộ dữ liệu có thể có tên cột `CustomerID` hoặc `Customer ID`; chương trình chuẩn hóa tên cột. Nếu file của bạn dùng tên cột khác, cần sửa phần ánh xạ trong `src/common.py`.

## 4. Chạy

Cách nhanh nhất, từ thư mục gốc dự án:

```bash
python run_all.py --workers 4
```

| Tham số | Ý nghĩa |
|---|---|
| `--workers 4` | Beam DirectRunner chạy song song 4 tiến trình (`multi_processing`) |
| `--hdfs` | Đọc dữ liệu thô từ HDFS, ghi kết quả Beam lên HDFS rồi tải về (xem mục 5) |
| `--skip-benchmark` | Bỏ bước benchmark (mất vài phút) |

Hoặc chạy từng bước:

```bash
python -m src.data_prep.data_profiling
python -m src.data_prep.data_quality
python -m src.beam_pipelines.pipeline_clean
python -m src.beam_pipelines.pipeline_customer
python -m src.beam_pipelines.pipeline_trends
python -m src.analytics.customer_segmentation
python -m src.analytics.trend_visualization
python -m src.analytics.architecture_diagram
python -m src.analytics.benchmark --scales 1 5 10 --workers 4
```

Nếu chạy trong Spyder, mở từng file và bấm Run. Các file tự tìm thư mục gốc dự án.

Các pipeline Beam nhận thêm tham số runner, không cần sửa code:
```bash
python -m src.beam_pipelines.pipeline_clean --direct_num_workers=4 --direct_running_mode=multi_processing
python -m src.beam_pipelines.pipeline_clean --runner=FlinkRunner --flink_master=localhost:8081
python -m src.analytics.customer_segmentation --k 4      # ép số cụm, mặc định tự chọn theo Silhouette
```

## 5. Lưu trữ trên HDFS (cần Docker Desktop)

1. Cài Docker Desktop (cần WSL 2), rồi dựng cụm HDFS:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
   ```
   Mở http://localhost:9870 để xem namenode, datanode và các file.
2. Thêm dòng sau vào `C:\Windows\System32\drivers\etc\hosts` (mở Notepad bằng quyền Administrator).
   WebHDFS chuyển hướng việc đọc/ghi dữ liệu sang tên máy `datanode`, nên Windows cần biết tên này:
   ```
   127.0.0.1 datanode
   ```
3. Chạy toàn bộ quy trình trên HDFS:
   ```bash
   python run_all.py --hdfs --workers 4
   ```
   Script sẽ đưa `data/raw/*.csv` lên `/retail/raw`, cho Beam đọc `hdfs://retail/raw/...` và ghi
   `hdfs://retail/processed/...`, rồi tải kết quả về `data/processed` cho bước K-Means.

Thao tác tay với HDFS:
```bash
python -m src.data_prep.hdfs_tool put data/raw/customer.csv /retail/raw/customer.csv
python -m src.data_prep.hdfs_tool ls /retail
python -m src.data_prep.hdfs_tool get /retail/processed data/processed
```

## 6. Web kết quả

Sau khi chạy xong mục 4, mở `app_results.py` trong Spyder và bấm F5, hoặc chạy:

```bash
streamlit run app_results.py
```

Trình duyệt tự mở tại http://localhost:8501. Web gồm 5 tab: tổng quan số liệu, chất lượng dữ liệu & Beam, phân nhóm khách hàng, xu hướng mua sắm, hiệu năng.

## 7. Đầu ra chính

- `results/tables/data_profile.csv`, `data_quality_summary.csv` — khảo sát & chất lượng dữ liệu
- `data/processed/clean_transactions.csv|.parquet` — bản làm sạch bằng pandas (đối chứng)
- `data/processed/beam_clean_transactions-*.csv` — bản làm sạch bằng Beam
- `data/processed/customer_aggregates-*.csv` — RFM theo khách hàng (Beam)
- `data/processed/{month,product,country,category,weekday_hour}_trends-*.csv` — xu hướng (Beam)
- `results/tables/kmeans_k_selection.csv`, `cluster_summary.csv`, `customer_segments.csv` — K-Means
- `results/tables/benchmark.csv` — pandas vs Beam 1 worker vs Beam N worker
- `results/charts/*.png` — biểu đồ, gồm cả `architecture.png`

## 8. Lưu ý phương pháp

- Beam chạy bằng `DirectRunner`, có thể song song nhiều tiến trình trên **một máy**. Đây chưa phải cụm nhiều máy; chỉ ghi trong báo cáo là đã chạy Flink/Spark nếu nhóm thật sự dựng và chạy được.
- Cụm HDFS trong `docker/` là 1 namenode + 1 datanode trên cùng máy. Chỉ ghi là đã lưu trên HDFS sau khi chạy thành công `run_all.py --hdfs`.
- Doanh thu dòng = `Quantity * UnitPrice`. Mã hóa đơn bắt đầu bằng `C` được đánh dấu là hóa đơn hủy. Các dòng số lượng âm được tách/loại khỏi phân tích doanh số bán thông thường; cần báo cáo số lượng bản ghi này.
- Việc làm sạch được làm hai lần: pandas (`data_quality.py`) và Beam (`pipeline_clean.py`). Bản pandas là đối chứng; hai bản cho cùng số dòng.
- Danh mục sản phẩm được suy ra từ từ khóa trong `Description` (`CATEGORY_RULES` trong `src/common.py`), không phải danh mục gốc.
- K-Means được chạy trên bảng đặc trưng RFM đã tổng hợp. Beam xử lý và tổng hợp dữ liệu; scikit-learn huấn luyện mô hình. k được chọn theo Silhouette cao nhất trong khoảng 2–8.
- Benchmark nhân bản file gốc (x5, x10...) để thử tải. Dữ liệu nhân bản không phải giao dịch kinh doanh thật, cần ghi rõ trong báo cáo.
