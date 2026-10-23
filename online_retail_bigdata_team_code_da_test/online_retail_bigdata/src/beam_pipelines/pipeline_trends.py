import sys
import argparse
import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import apache_beam as beam
from src.common import (PROCESSED_DIR, resolve_path, join_path, is_remote, find_beam_output,
                        make_pipeline_options, categorize, to_csv_line)

# Mỗi loại xu hướng -> một file <kind>_trends-00000-of-00001.csv
TREND_KINDS = ("month", "product", "country", "category", "weekday_hour")

def parse_row(line):
    try:
        row = next(csv.reader([line]))
        if len(row) < 9 or row[0] == "InvoiceNo":
            return
        invoice, stock, desc, qty, date, price, customer, country, revenue = row[:9]
        try:
            dt = datetime.fromisoformat(date.strip())
            q, rev = float(qty), float(revenue)
        except Exception:
            return
        yield ("month", dt.strftime("%Y-%m"), rev, q)
        yield ("product", stock.strip(), rev, q)
        yield ("country", country.strip() or "Unknown", rev, q)
        yield ("category", categorize(desc), rev, q)
        # Khóa "thứ-giờ": "0-10" = Thứ 2 lúc 10h
        yield ("weekday_hour", f"{dt.weekday()}-{dt.hour}", rev, q)
    except Exception:
        return

def sum_pairs(values):
    revenue, quantity = 0.0, 0.0
    for r, q in values:
        revenue += r
        quantity += q
    return (revenue, quantity)

def main():
    parser = argparse.ArgumentParser(description="Beam: tổng hợp xu hướng")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output-dir", default=str(PROCESSED_DIR))
    args, beam_args = parser.parse_known_args()

    path = resolve_path(args.input) if args.input else str(find_beam_output("beam_clean_transactions"))
    outdir = resolve_path(args.output_dir)
    if not is_remote(outdir):
        Path(outdir).mkdir(parents=True, exist_ok=True)

    with beam.Pipeline(options=make_pipeline_options(beam_args)) as p:
        rows = (
            p
            | "Read clean data" >> beam.io.ReadFromText(path, skip_header_lines=1)
            | "Parse trend rows" >> beam.FlatMap(parse_row)
        )
        for kind in TREND_KINDS:
            (
                rows
                | f"Filter {kind}" >> beam.Filter(lambda x, k=kind: x[0] == k)
                | f"Key {kind}" >> beam.Map(lambda x: (x[1], (x[2], x[3])))
                | f"Sum {kind}" >> beam.CombinePerKey(sum_pairs)
                | f"Format {kind}" >> beam.Map(lambda kv: to_csv_line([kv[0], kv[1][0], kv[1][1]]))
                | f"Write {kind}" >> beam.io.WriteToText(
                    join_path(outdir, f"{kind}_trends"),
                    file_name_suffix=".csv",
                    header=f"{kind},revenue,quantity",
                    num_shards=1
                )
            )
    print(f"Đã ghi các bảng xu hướng vào {outdir}")

if __name__ == "__main__":
    main()
