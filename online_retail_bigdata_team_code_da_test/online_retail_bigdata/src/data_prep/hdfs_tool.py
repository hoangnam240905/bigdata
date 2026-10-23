"""Đưa dữ liệu lên / tải kết quả từ HDFS qua WebHDFS (cổng 9870).

Ví dụ:
    python -m src.data_prep.hdfs_tool put data/raw/customer.csv /retail/raw/customer.csv
    python -m src.data_prep.hdfs_tool ls /retail
    python -m src.data_prep.hdfs_tool get /retail/processed data/processed
"""
import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common import resolve_path

DEFAULT_URL = "http://localhost:9870"
DEFAULT_USER = "root"


def get_client(url, user):
    try:
        from hdfs import InsecureClient
    except ImportError as exc:
        raise SystemExit("Thiếu thư viện hdfs. Chạy: python -m pip install hdfs") from exc
    return InsecureClient(url, user=user)


def main():
    parser = argparse.ArgumentParser(description="Thao tác HDFS qua WebHDFS")
    parser.add_argument("--url", default=DEFAULT_URL, help="Địa chỉ WebHDFS của namenode")
    parser.add_argument("--user", default=DEFAULT_USER)
    sub = parser.add_subparsers(dest="command", required=True)
    put = sub.add_parser("put", help="Tải file/thư mục local lên HDFS")
    put.add_argument("local")
    put.add_argument("hdfs_path")
    get = sub.add_parser("get", help="Tải file/thư mục từ HDFS về máy")
    get.add_argument("hdfs_path")
    get.add_argument("local")
    ls = sub.add_parser("ls", help="Liệt kê thư mục HDFS (đệ quy)")
    ls.add_argument("hdfs_path", nargs="?", default="/")
    args = parser.parse_args()

    client = get_client(args.url, args.user)
    if args.command == "put":
        parent = args.hdfs_path.rsplit("/", 1)[0] or "/"
        client.makedirs(parent)
        client.upload(args.hdfs_path, resolve_path(args.local), overwrite=True)
        print(f"Đã tải lên hdfs://{args.hdfs_path}")
    elif args.command == "get":
        local = Path(resolve_path(args.local))
        local.mkdir(parents=True, exist_ok=True)
        # Tải từng file vào thẳng thư mục local
        if client.status(args.hdfs_path)["type"] == "DIRECTORY":
            names = [n for n, s in client.list(args.hdfs_path, status=True) if s["type"] == "FILE"]
            sources = [args.hdfs_path.rstrip("/") + "/" + n for n in names]
        else:
            sources = [args.hdfs_path]
        for src in sources:
            client.download(src, str(local / src.rsplit("/", 1)[-1]), overwrite=True)
            print(f"Đã tải {src} về {local}")
    else:
        for path, _dirs, files in client.walk(args.hdfs_path, status=True):
            for name, status in files:
                print(f"{path[0].rstrip('/')}/{name}\t{status['length'] / 1024 ** 2:.2f} MB"
                      f"\treplication={status['replication']}\tblock={status['blockSize'] // 1024 ** 2} MB")


if __name__ == "__main__":
    main()
