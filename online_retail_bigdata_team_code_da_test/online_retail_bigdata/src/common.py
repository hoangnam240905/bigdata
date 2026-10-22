from pathlib import Path
import csv
import io
import pandas as pd
import re

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
TABLES_DIR = ROOT / "results" / "tables"
CHARTS_DIR = ROOT / "results" / "charts"
for _p in (RAW_DIR, PROCESSED_DIR, TABLES_DIR, CHARTS_DIR):
    _p.mkdir(parents=True, exist_ok=True)

COLUMN_ALIASES = {
    "invoiceno": "InvoiceNo",
    "invoice": "InvoiceNo",
    "stockcode": "StockCode",
    "description": "Description",
    "quantity": "Quantity",
    "invoicedate": "InvoiceDate",
    "unitprice": "UnitPrice",
    "customerid": "CustomerID",
    "country": "Country",
}

# Bộ dữ liệu gốc không có cột danh mục: suy ra danh mục từ từ khóa trong Description.
# Quy tắc được xét theo thứ tự, quy tắc khớp đầu tiên thắng.
CATEGORY_RULES = [
    ("Giáng sinh & lễ hội", r"CHRISTMAS|XMAS|EASTER|ADVENT|SANTA|REINDEER"),
    ("Đèn - nến - đèn lồng", r"T-LIGHT|CANDLE|LANTERN|LIGHT|LAMP"),
    ("Túi xách & túi vải", r"\bBAG\b|BAGS|JUMBO|TOTE|SHOPPER"),
    ("Nhà bếp & bàn ăn", r"MUG|CUP|PLATE|BOWL|TEA|CAKE|BAKING|JAR|SPOON|NAPKIN|LUNCH|BOTTLE|KITCHEN|APRON"),
    ("Giấy - thiệp - gói quà", r"CARD|WRAP|PAPER|NOTEBOOK|PENCIL|PEN\b|STICKER|ENVELOPE|RIBBON|TAPE"),
    ("Hộp & đồ lưu trữ", r"\bBOX\b|BOXES|\bTIN\b|TINS|STORAGE|BASKET|CASE|DRAWER"),
    ("Đồ chơi & trẻ em", r"TOY|DOLL|GAME|CHILDREN|CHILDS|PLAYHOUSE|SPACEBOY"),
    ("Trang trí nhà cửa", r"HEART|SIGN|FRAME|CLOCK|DECORATION|HANGING|WREATH|GARLAND|BUNTING|ORNAMENT|CUSHION|DOORMAT|VINTAGE"),
    ("Trang sức & phụ kiện", r"NECKLACE|BRACELET|EARRING|RING\b|HAIR|PURSE|UMBRELLA"),
    ("Phí & dịch vụ", r"POSTAGE|MANUAL|CARRIAGE|DOTCOM|AMAZON FEE|BANK CHARGES"),
]
_CATEGORY_PATTERNS = [(name, re.compile(pattern)) for name, pattern in CATEGORY_RULES]

WEEKDAYS = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"]


def categorize(description):
    text = str(description).upper()
    for name, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            return name
    return "Khác"


def to_csv_line(values):
    """Ghi một dòng CSV có quote đúng chuẩn (dùng trong Beam trước WriteToText)."""
    buffer = io.StringIO()
    csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="").writerow(values)
    return buffer.getvalue()


def is_remote(path):
    """Đường dẫn dạng hdfs://, gs://, s3://... do Beam FileSystems xử lý."""
    return "://" in str(path)


def resolve_path(path):
    """Trả về chuỗi đường dẫn: giữ nguyên nếu là hdfs://..., còn lại tính theo thư mục gốc dự án."""
    if is_remote(path):
        return str(path)
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    return str(path)


def join_path(base, name):
    if is_remote(base):
        return str(base).rstrip("/") + "/" + name
    return str(Path(base) / name)


def find_beam_output(prefix, directory=PROCESSED_DIR):
    files = sorted(Path(directory).glob(prefix + "*.csv"))
    if not files:
        raise FileNotFoundError(f"Không tìm thấy {prefix}*.csv trong {directory}. Hãy chạy pipeline Beam tương ứng.")
    return files[0]


def make_pipeline_options(beam_args):
    """Mặc định chạy DirectRunner; có thể đổi bằng tham số, ví dụ:
    --direct_num_workers=4 --direct_running_mode=multi_processing  (song song nhiều tiến trình)
    --runner=FlinkRunner --flink_master=localhost:8081             (cụm Flink)
    --hdfs_host=localhost --hdfs_port=9870 --hdfs_user=root          (đọc/ghi hdfs://)
    """
    from apache_beam.options.pipeline_options import PipelineOptions
    if not any(arg.startswith("--runner") for arg in beam_args):
        # Từ Beam 2.6x, DirectRunner tự chuyển sang PrismRunner và bỏ qua --direct_num_workers.
        # Khi chỉ định số worker, dùng thẳng FnApiRunner (lõi của DirectRunner Python) để
        # chế độ multi_processing thực sự chia việc cho nhiều tiến trình.
        if any(arg.startswith("--direct_num_workers") for arg in beam_args):
            runner = "apache_beam.runners.portability.fn_api_runner.fn_runner.FnApiRunner"
        else:
            runner = "DirectRunner"
        beam_args = [f"--runner={runner}", *beam_args]
    return PipelineOptions(beam_args)


def normalize_columns(df):
    rename = {}
    for col in df.columns:
        key = re.sub(r"[^a-z0-9]", "", str(col).lower())
        if key in COLUMN_ALIASES:
            rename[col] = COLUMN_ALIASES[key]
    return df.rename(columns=rename)

def find_input_file(input_path=None):
    if input_path:
        if is_remote(input_path):
            return str(input_path)
        path = Path(input_path)
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {path}")
        return path
    candidates = sorted(RAW_DIR.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"Chưa có CSV trong {RAW_DIR}. Hãy tải Online Retail CSV từ Kaggle vào thư mục này."
        )
    return candidates[0]

def read_retail_csv(input_path=None):
    path = find_input_file(input_path)
    # latin1 thường tương thích với bản Online Retail gốc; fallback utf-8-sig
    try:
        df = pd.read_csv(path, encoding="latin1", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df = normalize_columns(df)
    required = {"InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Thiếu cột bắt buộc: {sorted(missing)}. Các cột hiện có: {list(df.columns)}"
        )
    if "Description" not in df.columns:
        df["Description"] = ""
    if "Country" not in df.columns:
        df["Country"] = "Unknown"
    return df, path
