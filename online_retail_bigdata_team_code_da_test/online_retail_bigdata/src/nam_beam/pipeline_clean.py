import sys
import argparse
import csv
from pathlib import Path

# Cho phép bấm Run trực tiếp trong Spyder (thêm thư mục gốc dự án vào sys.path)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import apache_beam as beam
from src.common import (find_input_file, PROCESSED_DIR, resolve_path, join_path,
                        is_remote, make_pipeline_options, to_csv_line)

OUTPUT_COLUMNS = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
                  "UnitPrice", "CustomerID", "Country", "line_revenue"]

def parse_and_clean(line):
    try:
        row = next(csv.reader([line]))
        if len(row) < 8:
            return
        # Header is skipped separately. Expected order from standard Online Retail.
        invoice, stock, description, quantity, invoice_date, unit_price, customer_id, country = row[:8]
        invoice = invoice.strip()
        if invoice.lower() == "invoiceno":
            return
        if invoice.upper().startswith("C"):
            return
        try:
            quantity = float(quantity)
            unit_price = float(unit_price)
            customer_id_float = float(customer_id)
        except (ValueError, TypeError):
            return
        if quantity <= 0 or unit_price <= 0:
            return
        if not customer_id or customer_id.strip() == "" or customer_id_float <= 0:
            return
        # ISO-like string is retained; pandas will parse it downstream.
        date = invoice_date.strip()
        if not date:
            return
        revenue = quantity * unit_price
        yield {
            "InvoiceNo": invoice,
            "StockCode": stock.strip(),
            "Description": description.strip(),
            "Quantity": quantity,
            "InvoiceDate": date,
            "UnitPrice": unit_price,
            "CustomerID": str(int(customer_id_float)),
            "Country": country.strip(),
            "line_revenue": revenue,
        }
    except Exception:
        return

def main():
    parser = argparse.ArgumentParser(description="Nam: Apache Beam clean pipeline")
    parser.add_argument("--input", default=None, help="File CSV local hoặc hdfs://...")
    parser.add_argument("--output-dir", default=str(PROCESSED_DIR), help="Thư mục local hoặc hdfs://...")
    known, beam_args = parser.parse_known_args()
    input_file = str(find_input_file(known.input))
    output_dir = resolve_path(known.output_dir)
    if not is_remote(output_dir):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_prefix = join_path(output_dir, "beam_clean_transactions")

    with beam.Pipeline(options=make_pipeline_options(beam_args)) as p:
        (
            p
            | "Read CSV lines" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Parse and filter" >> beam.FlatMap(parse_and_clean)
            | "Encode CSV line" >> beam.Map(lambda r: to_csv_line([r[c] for c in OUTPUT_COLUMNS]))
            | "Write cleaned CSV" >> beam.io.WriteToText(
                output_prefix, file_name_suffix=".csv", header=",".join(OUTPUT_COLUMNS),
                num_shards=1
            )
        )
    print(f"Pipeline hoàn tất. File đầu ra có tiền tố: {output_prefix}")

if __name__ == "__main__":
    main()
