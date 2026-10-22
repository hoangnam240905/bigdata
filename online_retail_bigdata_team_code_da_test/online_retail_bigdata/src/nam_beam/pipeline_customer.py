import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import apache_beam as beam
from src.common import (PROCESSED_DIR, resolve_path, join_path, is_remote,
                        find_beam_output, make_pipeline_options)

def parse_customer(line):
    import csv
    from datetime import datetime
    try:
        row = next(csv.reader([line]))
        if len(row) < 9 or row[0] == "InvoiceNo":
            return
        invoice, _stock, _desc, qty, date, price, customer, _country, revenue = row[:9]
        customer = customer.strip()
        if not customer:
            return
        try:
            dt = datetime.fromisoformat(date.strip())
            q, rev = float(qty), float(revenue)
        except Exception:
            return
        yield (customer, (dt, invoice, q, rev))
    except Exception:
        return

class CustomerCombineFn(beam.CombineFn):
    """Tổng hợp RFM theo khách hàng. Dùng CombineFn thay vì GroupByKey để Beam
    tổng hợp cục bộ trên từng worker trước khi trộn (combiner lifting), giảm dữ liệu shuffle."""

    def create_accumulator(self):
        # (latest_datetime, invoice_set, quantity_sum, monetary_sum)
        return (None, set(), 0.0, 0.0)

    def add_input(self, acc, value):
        dt, invoice, qty, rev = value
        latest = dt if acc[0] is None or dt > acc[0] else acc[0]
        acc[1].add(invoice)
        return (latest, acc[1], acc[2] + qty, acc[3] + rev)

    def merge_accumulators(self, accumulators):
        latest, invoices, qty, rev = None, set(), 0.0, 0.0
        for a in accumulators:
            if a[0] is not None and (latest is None or a[0] > latest):
                latest = a[0]
            invoices |= a[1]
            qty += a[2]
            rev += a[3]
        return (latest, invoices, qty, rev)

    def extract_output(self, acc):
        return {
            "last_purchase": acc[0].date().isoformat(),
            "invoice_count": len(acc[1]),
            "quantity_sum": acc[2],
            "monetary": acc[3],
        }

def main():
    parser = argparse.ArgumentParser(description="Nam: Beam aggregate by customer")
    parser.add_argument("--input", default=None, help="Clean CSV from pipeline_clean (local hoặc hdfs://)")
    parser.add_argument("--output-dir", default=str(PROCESSED_DIR))
    args, beam_args = parser.parse_known_args()

    input_path = resolve_path(args.input) if args.input else str(find_beam_output("beam_clean_transactions"))
    outdir = resolve_path(args.output_dir)
    if not is_remote(outdir):
        Path(outdir).mkdir(parents=True, exist_ok=True)

    with beam.Pipeline(options=make_pipeline_options(beam_args)) as p:
        (
            p
            | "Read clean CSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "Parse customer" >> beam.FlatMap(parse_customer)
            | "Combine per customer" >> beam.CombinePerKey(CustomerCombineFn())
            | "Format customer CSV" >> beam.Map(lambda kv: ",".join([
                kv[0], kv[1]["last_purchase"], str(kv[1]["invoice_count"]),
                str(kv[1]["quantity_sum"]), str(kv[1]["monetary"])
            ]))
            | "Write customer aggregates" >> beam.io.WriteToText(
                join_path(outdir, "customer_aggregates"),
                file_name_suffix=".csv",
                header="CustomerID,last_purchase,invoice_count,quantity_sum,monetary",
                num_shards=1
            )
        )
    print(f"Đã ghi bảng tổng hợp khách hàng vào {outdir}")

if __name__ == "__main__":
    main()
