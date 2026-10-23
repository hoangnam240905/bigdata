import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from src.common import read_retail_csv, TABLES_DIR, PROCESSED_DIR

def main():
    parser = argparse.ArgumentParser(description="Kiểm tra chất lượng và xuất Parquet")
    parser.add_argument("--input", default=None)
    args, _ = parser.parse_known_args()

    df, path = read_retail_csv(args.input)
    before = len(df)

    df["InvoiceNo"] = df["InvoiceNo"].astype("string").str.strip()
    df["StockCode"] = df["StockCode"].astype("string").str.strip()
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
    df["is_cancelled_invoice"] = df["InvoiceNo"].str.upper().str.startswith("C", na=False)
    df["line_revenue"] = df["Quantity"] * df["UnitPrice"]

    invalid_date = int(df["InvoiceDate"].isna().sum())
    invalid_numeric = int(df[["Quantity", "UnitPrice"]].isna().any(axis=1).sum())
    missing_customer = int(df["CustomerID"].isna().sum())
    exact_duplicates = int(df.duplicated().sum())
    negative_quantity = int((df["Quantity"] < 0).sum())
    cancelled = int(df["is_cancelled_invoice"].sum())

    # Bỏ hóa đơn hủy, SL/giá <= 0, thiếu khách, ngày/số lỗi (dòng trùng chỉ báo cáo)
    valid = df[
        df["InvoiceDate"].notna()
        & df["Quantity"].notna()
        & df["UnitPrice"].notna()
        & (df["Quantity"] > 0)
        & (df["UnitPrice"] > 0)
        & (~df["is_cancelled_invoice"])
        & (df["CustomerID"].notna())
    ].copy()
    valid["CustomerID"] = valid["CustomerID"].astype("int64")
    valid["InvoiceNo"] = valid["InvoiceNo"].astype(str)
    valid["month"] = valid["InvoiceDate"].dt.to_period("M").astype(str)

    csv_out = PROCESSED_DIR / "clean_transactions.csv"
    valid.to_csv(csv_out, index=False, encoding="utf-8-sig")

    parquet_out = PROCESSED_DIR / "clean_transactions.parquet"
    parquet_status = "OK"
    try:
        valid.to_parquet(parquet_out, index=False)
    except Exception as exc:
        parquet_status = f"Không ghi được Parquet: {exc}. Kiểm tra pyarrow."

    summary = pd.DataFrame([
        {"metric": "source_file", "value": str(path)},
        {"metric": "rows_before", "value": before},
        {"metric": "rows_after_sales_filter", "value": len(valid)},
        {"metric": "invalid_dates", "value": invalid_date},
        {"metric": "invalid_quantity_or_price", "value": invalid_numeric},
        {"metric": "missing_customer_id", "value": missing_customer},
        {"metric": "exact_duplicate_rows", "value": exact_duplicates},
        {"metric": "negative_quantity_rows", "value": negative_quantity},
        {"metric": "cancelled_invoice_rows", "value": cancelled},
        {"metric": "parquet_status", "value": parquet_status},
    ])
    out = TABLES_DIR / "data_quality_summary.csv"
    summary.to_csv(out, index=False, encoding="utf-8-sig")

    print(summary.to_string(index=False))
    print(f"\nCSV sạch: {csv_out}")
    print(f"Parquet: {parquet_out} ({parquet_status})")
    print("Lưu ý: quy tắc lọc doanh số thông thường cần được mô tả trong báo cáo.")

if __name__ == "__main__":
    main()
