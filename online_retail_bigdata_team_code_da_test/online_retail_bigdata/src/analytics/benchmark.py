"""Đo thời gian làm sạch dữ liệu: pandas vs Beam 1 worker vs Beam N worker.
Dữ liệu x5, x10... là bản nhân bản file gốc để thử tải, không phải giao dịch thật.
"""
import os
import sys
import time
import shutil
import argparse
import subprocess
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from src.common import ROOT, find_input_file, TABLES_DIR, CHARTS_DIR

BENCH_DIR = ROOT / "data" / "benchmark"


def make_scaled_file(source, scale):
    """Tạo file = header + phần thân của file gốc lặp lại `scale` lần."""
    if scale == 1:
        return source
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    target = BENCH_DIR / f"retail_x{scale}.csv"
    with open(source, "rb") as f:
        header = f.readline()
        body = f.read()
    if not body.endswith(b"\n"):
        body += b"\n"
    expected = len(header) + len(body) * scale
    if target.exists() and target.stat().st_size == expected:
        return target
    with open(target, "wb") as out:
        out.write(header)
        for _ in range(scale):
            out.write(body)
    return target


def count_rows(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f) - 1


def run_pandas(path, outdir):
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df.columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                  "UnitPrice", "CustomerID", "Country"]
    df = df[~df["InvoiceNo"].astype(str).str.upper().str.startswith("C")
            & (df["Quantity"] > 0) & (df["UnitPrice"] > 0) & df["CustomerID"].notna()].copy()
    df["CustomerID"] = df["CustomerID"].astype("int64")
    df["line_revenue"] = df["Quantity"] * df["UnitPrice"]
    df.to_csv(Path(outdir) / "pandas_clean.csv", index=False)


def run_beam(path, outdir, workers):
    cmd = [sys.executable, "-m", "src.beam_pipelines.pipeline_clean", "--input", str(path), "--output-dir", str(outdir)]
    # Cả 2 cấu hình Beam cùng dùng FnApiRunner để so sánh công bằng
    cmd += [f"--direct_num_workers={workers}"]
    if workers > 1:
        cmd += ["--direct_running_mode=multi_processing"]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    subprocess.run(cmd, cwd=ROOT, check=True, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def plot(results):
    fig, ax = plt.subplots(figsize=(10, 5))
    for method, group in results.groupby("method", sort=False):
        ax.plot(group["input_rows"] / 1e6, group["elapsed_seconds"], marker="o", label=method)
    ax.set_xlabel("Số dòng đầu vào (triệu)")
    ax.set_ylabel("Thời gian làm sạch (giây)")
    ax.set_title("Thời gian xử lý khi dữ liệu tăng")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / "benchmark.png", dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Benchmark pandas vs Apache Beam")
    parser.add_argument("--input", default=None)
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 5, 10], help="Hệ số nhân bản dữ liệu")
    parser.add_argument("--workers", type=int, default=4, help="Số worker cho Beam multi_processing")
    args, _ = parser.parse_known_args()
    source = find_input_file(args.input)

    methods = [("pandas (1 tiến trình)", lambda p, o: run_pandas(p, o)),
               ("Beam 1 worker", lambda p, o: run_beam(p, o, 1)),
               (f"Beam {args.workers} worker (multi_processing)", lambda p, o: run_beam(p, o, args.workers))]
    records = []
    for scale in args.scales:
        path = make_scaled_file(source, scale)
        rows = count_rows(path)
        size_mb = path.stat().st_size / 1024 ** 2
        for name, fn in methods:
            outdir = tempfile.mkdtemp(prefix="bench_")
            start = time.perf_counter()
            fn(path, outdir)
            elapsed = time.perf_counter() - start
            shutil.rmtree(outdir, ignore_errors=True)
            records.append({
                "method": name, "scale": scale, "input_rows": rows, "input_mb": round(size_mb, 1),
                "elapsed_seconds": round(elapsed, 2), "rows_per_second": round(rows / elapsed),
            })
            print(f"x{scale:<3} {rows:>10,} dòng | {name:<28} | {elapsed:7.2f} s", flush=True)

    results = pd.DataFrame(records)
    out = TABLES_DIR / "benchmark.csv"
    results.to_csv(out, index=False, encoding="utf-8-sig")
    plot(results)
    print(f"\nĐã lưu: {out} và {CHARTS_DIR / 'benchmark.png'}")
    print("Lưu ý: dữ liệu x5, x10... là bản nhân bản để thử tải, không phải giao dịch thật.")


if __name__ == "__main__":
    main()
