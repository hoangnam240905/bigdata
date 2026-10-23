import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common import read_retail_csv, TABLES_DIR
def main():
    parser = argparse.ArgumentParser(description="Khảo sát dữ liệu Online Retail")
    parser.add_argument("--input", default=None)
    args, _ = parser.parse_known_args()

    df, path = read_retail_csv(args.input)
    profile = []
    for col in df.columns:
        profile.append({
            "column": col,
            "dtype": str(df[col].dtype),
            "non_null": int(df[col].notna().sum()),
            "missing": int(df[col].isna().sum()),
            "unique_values": int(df[col].nunique(dropna=True)),
        })
    out = TABLES_DIR / "data_profile.csv"
    import pandas as pd
    pd.DataFrame(profile).to_csv(out, index=False, encoding="utf-8-sig")

    print(f"File: {path}")
    print(f"Số dòng: {len(df):,} | Số cột: {len(df.columns)}")
    print(f"Đã lưu bảng khảo sát: {out}")
    print(df.head())
    print("\nSố giá trị thiếu theo cột:")
    print(df.isna().sum())

if __name__ == "__main__":
    main()
