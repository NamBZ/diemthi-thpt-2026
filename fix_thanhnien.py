import argparse
import csv
import random
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter

API_URL = "https://thanhnien.vn/api/get-data-tuyen-sinh.htm"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Cache-Control": "no-cache",
    "Origin": "https://thanhnien.vn",
    "Pragma": "no-cache",
    "Referer": "https://thanhnien.vn/",
    "Sec-Ch-Ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    ),
}

FIELDNAMES = [
    "NAM", "KY_THI", "SOBAODANH", "HO_TEN", "NGAY_SINH",
    "DM1", "DM2", "DM3", "DM4", "DM5", "DM6", "DM7", "DM8", "DM9", "DM10",
    "DM11", "DM12", "DM13", "DM14", "DM15", "DM16", "DM17", "DM18", "DM19", "DM20",
    "TONGDIEM", "NGOAINGU", "Id", "file_name", "modified_date",
]

SBD_RE = re.compile(r"\b\d{8}\b")


def normalize_proxy(proxy: str) -> str:
    proxy = proxy.strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks4://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


def load_proxies(proxy_file: str) -> list[str]:
    path = Path(proxy_file)
    if not path.exists():
        return []

    proxies: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        proxy = normalize_proxy(line)
        if proxy and proxy not in seen:
            proxies.append(proxy)
            seen.add(proxy)
    return proxies


def build_proxies(proxy: str | None):
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def read_error_sbd(error_file: str) -> list[str]:
    """Đọc SBD từ errors_thanhnien.txt. Hỗ trợ cả dòng chỉ có SBD hoặc dòng log có chứa SBD 8 số."""
    path = Path(error_file)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file lỗi: {error_file}")

    result: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = SBD_RE.search(line)
        if not match:
            continue
        sbd = match.group(0)
        if sbd not in seen:
            result.append(sbd)
            seen.add(sbd)
    return result


def load_existing_sbd(output_csv: str) -> set[str]:
    """Đọc các SBD đã có trong CSV để tránh ghi trùng."""
    path = Path(output_csv)
    if not path.exists() or path.stat().st_size == 0:
        return set()

    existing: set[str] = set()
    with open(output_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sbd = (row.get("SOBAODANH") or "").strip()
            if sbd:
                existing.add(sbd)
    return existing


def init_csv(output_csv: str) -> None:
    path = Path(output_csv)
    if not path.exists() or path.stat().st_size == 0:
        with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def append_rows(output_csv: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    init_csv(output_csv)
    with open(output_csv, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        for item in rows:
            writer.writerow({field: item.get(field, "") for field in FIELDNAMES})


def parse_thanhnien_payload(payload: Any):
    if not isinstance(payload, dict):
        raise ValueError(f"Response không phải JSON object: {type(payload)}")

    if payload.get("success") is False:
        raise requests.exceptions.RequestException(f"API trả success=false: {payload}")

    rows = payload.get("data", [])
    total = payload.get("total", len(rows) if isinstance(rows, list) else 0)

    if isinstance(rows, list) and total > 0 and rows:
        return rows

    return 404


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_score(
    sbd: str,
    *,
    pageindex: int,
    size: int,
    type_id: int,
    retries: int,
    timeout: float,
    proxies: list[str],
):
    session = make_session()
    max_attempts = retries + 1
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        proxy = random.choice(proxies) if proxies and attempt > 1 else None

        try:
            response = session.get(
                API_URL,
                params={
                    "keywords": sbd,
                    "pageindex": pageindex,
                    "size": size,
                    "type": type_id,
                },
                timeout=(3.05, timeout),
                proxies=build_proxies(proxy),
            )

            if response.status_code == 404:
                return "404", None, None

            if response.status_code == 429:
                raise requests.exceptions.RequestException(f"HTTP 429 Too Many Requests cho SBD {sbd}")

            response.raise_for_status()
            parsed = parse_thanhnien_payload(response.json())
            if parsed == 404:
                return "404", None, None
            return "data", parsed, None

        except (requests.exceptions.RequestException, ValueError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep_time = min(0.6 * attempt + random.uniform(0, 0.5), 3.0)
            time.sleep(sleep_time)

    return "error", None, str(last_error) if last_error else "unknown error"


def write_list(path: str, values: list[str]) -> None:
    Path(path).write_text("\n".join(values) + ("\n" if values else ""), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry các SBD lỗi từ errors_thanhnien.txt")
    parser.add_argument("--error-file", default="errors_thanhnien.txt", help="File chứa SBD lỗi cần retry")
    parser.add_argument("--output", default="diem_thi_thanhnien.csv", help="CSV kết quả để append dữ liệu thành công")
    parser.add_argument("--remaining-error-file", default="errors_thanhnien_retry_failed.txt", help="File ghi các SBD vẫn lỗi sau retry")
    parser.add_argument("--not-found-file", default="errors_thanhnien_404.txt", help="File ghi SBD trả 404/không có dữ liệu")
    parser.add_argument("--proxy-file", default="proxies.txt", help="File proxy, nếu có")
    parser.add_argument("--max-workers", type=int, default=15, help="Số luồng retry song song")
    parser.add_argument("--retries", type=int, default=5, help="Số lần retry mỗi SBD sau lần gọi đầu")
    parser.add_argument("--timeout", type=float, default=8, help="Read timeout mỗi request, giây")
    parser.add_argument("--pageindex", type=int, default=1)
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--type", dest="type_id", type=int, default=3)
    parser.add_argument("--no-skip-existing", action="store_true", help="Không bỏ qua SBD đã có trong CSV")
    parser.add_argument("--replace-error-file", action="store_true", help="Ghi đè error-file bằng danh sách lỗi còn lại sau khi retry")
    args = parser.parse_args()

    if args.max_workers < 1:
        raise ValueError("--max-workers phải >= 1")
    if args.retries < 0:
        raise ValueError("--retries phải >= 0")

    sbd_list = read_error_sbd(args.error_file)
    print(f"Đọc được {len(sbd_list):,} SBD lỗi từ {args.error_file}")

    if not args.no_skip_existing:
        existing_sbd = load_existing_sbd(args.output)
        before = len(sbd_list)
        sbd_list = [sbd for sbd in sbd_list if sbd not in existing_sbd]
        print(f"Bỏ qua {before - len(sbd_list):,} SBD đã có trong {args.output}")

    proxies = load_proxies(args.proxy_file)
    if proxies:
        print(f"Đã tải {len(proxies):,} proxy từ {args.proxy_file}")
    else:
        print("Không có proxy, retry bằng request trực tiếp.")

    if not sbd_list:
        print("Không còn SBD nào cần retry.")
        write_list(args.remaining_error_file, [])
        write_list(args.not_found_file, [])
        return

    success_count = 0
    row_count = 0
    failed_sbd: list[str] = []
    not_found_sbd: list[str] = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                get_score,
                sbd,
                pageindex=args.pageindex,
                size=args.size,
                type_id=args.type_id,
                retries=args.retries,
                timeout=args.timeout,
                proxies=proxies,
            ): sbd
            for sbd in sbd_list
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            sbd = futures[future]
            try:
                status, rows, err = future.result()
            except Exception as exc:
                status, rows, err = "error", None, str(exc)

            if status == "data" and rows:
                append_rows(args.output, rows)
                success_count += 1
                row_count += len(rows)
                print(f"OK {sbd} -> ghi {len(rows)} dòng")
            elif status == "404":
                not_found_sbd.append(sbd)
                print(f"404 {sbd}")
            else:
                failed_sbd.append(sbd)
                print(f"FAIL {sbd}: {err}")

            if idx % 100 == 0 or idx == len(sbd_list):
                print(
                    f"Tiến độ {idx:,}/{len(sbd_list):,} | "
                    f"OK={success_count:,}, rows={row_count:,}, "
                    f"404={len(not_found_sbd):,}, lỗi còn lại={len(failed_sbd):,}"
                )

    write_list(args.remaining_error_file, failed_sbd)
    write_list(args.not_found_file, not_found_sbd)

    if args.replace_error_file:
        src = Path(args.error_file)
        backup = src.with_suffix(src.suffix + ".bak")
        shutil.copyfile(src, backup)
        write_list(args.error_file, failed_sbd)
        print(f"Đã backup file lỗi cũ sang {backup} và ghi đè {args.error_file}")

    print("=" * 70)
    print(f"Tổng SBD retry: {len(sbd_list):,}")
    print(f"Thành công: {success_count:,} SBD, ghi {row_count:,} dòng vào {args.output}")
    print(f"Không có dữ liệu/404: {len(not_found_sbd):,} -> {args.not_found_file}")
    print(f"Vẫn lỗi request/JSON: {len(failed_sbd):,} -> {args.remaining_error_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
