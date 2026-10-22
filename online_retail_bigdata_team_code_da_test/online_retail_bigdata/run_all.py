"""Chạy toàn bộ quy trình của nhóm theo đúng thứ tự.

    python run_all.py                    # chạy local, Beam DirectRunner 1 worker
    python run_all.py --workers 4        # Beam chạy song song 4 tiến trình
    python run_all.py --hdfs             # đọc/ghi dữ liệu qua HDFS (cần chạy docker/docker-compose.yml)
    python run_all.py --skip-benchmark   # bỏ bước benchmark (tốn vài phút)
"""
import os
import sys
import time
import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from src.common import find_input_file

# Ép tiến trình con in UTF-8 để không lỗi tiếng Việt khi chuyển hướng output trên Windows
ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}
HDFS_ARGS = ["--hdfs_host=localhost", "--hdfs_port=9870", "--hdfs_user=root"]
HDFS_RAW = "/retail/raw"
HDFS_PROCESSED = "/retail/processed"
CLEAN_FILE = "beam_clean_transactions-00000-of-00001.csv"


def run(owner, title, module, *args):
    print(f"\n=== [{owner}] {title} ===", flush=True)
    start = time.perf_counter()
    subprocess.run([sys.executable, "-m", module, *args], cwd=ROOT, check=True, env=ENV)
    print(f"--- xong sau {time.perf_counter() - start:.1f} s", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Chạy toàn bộ pipeline Online Retail")
    parser.add_argument("--workers", type=int, default=1, help="Số worker cho Beam DirectRunner")
    parser.add_argument("--hdfs", action="store_true", help="Lưu trữ dữ liệu trên HDFS")
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()

    beam_args = []
    if args.workers > 1:
        beam_args += [f"--direct_num_workers={args.workers}", "--direct_running_mode=multi_processing"]

    raw_file = Path(find_input_file())
    run("Đạt", "Khảo sát dữ liệu", "src.dat_team.data_profiling")
    run("Đạt", "Kiểm tra chất lượng & xuất Parquet", "src.dat_team.data_quality")

    if args.hdfs:
        hdfs_raw = f"{HDFS_RAW}/{raw_file.name}"
        run("Đạt", "Đưa dữ liệu thô lên HDFS", "src.dat_team.hdfs_tool", "put", str(raw_file), hdfs_raw)
        beam_args += HDFS_ARGS
        clean_in = ["--input", f"hdfs:/{hdfs_raw}"]
        processed = ["--output-dir", f"hdfs:/{HDFS_PROCESSED}"]
        clean_file = ["--input", f"hdfs:/{HDFS_PROCESSED}/{CLEAN_FILE}"]
    else:
        clean_in, processed, clean_file = [], [], []

    run("Nam", "Beam: làm sạch & biến đổi", "src.nam_beam.pipeline_clean", *clean_in, *processed, *beam_args)
    run("Nam", "Beam: tổng hợp theo khách hàng", "src.nam_beam.pipeline_customer", *clean_file, *processed, *beam_args)
    run("Nam", "Beam: tổng hợp xu hướng", "src.nam_beam.pipeline_trends", *clean_file, *processed, *beam_args)

    if args.hdfs:
        run("Đạt", "Tải kết quả Beam từ HDFS về máy", "src.dat_team.hdfs_tool", "get", HDFS_PROCESSED, "data/processed")

    run("Toàn", "RFM + K-Means", "src.toan_analytics.customer_segmentation")
    run("Toàn", "Trực quan hóa xu hướng", "src.toan_analytics.trend_visualization")
    run("Toàn", "Sơ đồ kiến trúc", "src.toan_analytics.architecture_diagram")
    if not args.skip_benchmark:
        run("Toàn", "Benchmark pandas vs Beam", "src.toan_analytics.benchmark", "--workers", str(max(args.workers, 4)))

    print("\nHoàn tất. Mở web trình chiếu: streamlit run app.py")


if __name__ == "__main__":
    main()
