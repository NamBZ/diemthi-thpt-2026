import argparse
import csv
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests


API_URL = "https://s6.tuoitre.vn/api/diem-thi-thpt.htm"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Cache-Control": "no-cache",
    "Origin": "https://tuoitre.vn",
    "Pragma": "no-cache",
    "Priority": "u=1, i",
    "Referer": "https://tuoitre.vn/",
    "Sec-Ch-Ua": '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/149.0.0.0 Safari/537.36"
    ),
}

FIELDNAMES = [
    "STT",
    "SOBAODANH",
    "TOAN",
    "VA",
    "LI",
    "HO",
    "SI",
    "SU",
    "DI",
    "KTPL",
    "TI",
    "CNCN",
    "CNNN",
    "NN",
    "MON_NN",
    "NGAY_SINH",
    "file_name",
]

write_lock = threading.Lock()
proxy_lock = threading.Lock()
session_local = threading.local()

PROXIES: list[str] = []
PROXY_CURSOR = 0
EXISTING_SBD: set[str] = set()


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


def pick_proxy(exclude: set[str] | None = None) -> str | None:
    global PROXY_CURSOR
    exclude = exclude or set()
    if not PROXIES:
        return None

    with proxy_lock:
        total = len(PROXIES)
        for _ in range(total):
            proxy = PROXIES[PROXY_CURSOR % total]
            PROXY_CURSOR += 1
            if proxy not in exclude:
                return proxy
    return None


def get_session() -> requests.Session:
    if not hasattr(session_local, "session"):
        session = requests.Session()
        session.headers.update(HEADERS)
        session_local.session = session
    return session_local.session


def init_csv(output_csv: str):
    path = Path(output_csv)
    if not path.exists():
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def load_existing_sbd(output_csv: str) -> set[str]:
    path = Path(output_csv)
    if not path.exists():
        return set()

    existing: set[str] = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sbd = (row.get("SOBAODANH") or "").strip()
                if sbd:
                    existing.add(sbd)
    except OSError:
        return set()
    return existing


def read_error_sbd(error_file: str) -> list[str]:
    path = Path(error_file)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file lỗi: {error_file}")

    seen: set[str] = set()
    result: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        sbd = line.strip()
        if not sbd or sbd.startswith("#"):
            continue
        # Chỉ nhận SBD dạng 8 chữ số, ví dụ 01000123.
        if not (sbd.isdigit() and len(sbd) == 8):
            print(f"Bỏ qua dòng không hợp lệ trong {error_file}: {sbd}")
            continue
        if sbd not in seen:
            result.append(sbd)
            seen.add(sbd)
    return result


def get_score(sbd: str, year: int, retries: int, timeout: int):
    """
    Trả về:
    - list dữ liệu nếu có
    - 404 nếu API trả về data rỗng / total = 0 / HTTP 404
    - None nếu vẫn lỗi sau khi retry
    """
    session = get_session()
    used_proxies: set[str] = set()
    max_attempts = retries + 1

    for attempt in range(1, max_attempts + 1):
        proxy = None
        if attempt > 1:
            proxy = pick_proxy(used_proxies)
            if proxy:
                used_proxies.add(proxy)

        try:
            response = session.get(
                API_URL,
                params={"sbd": sbd, "year": year},
                timeout=timeout,
                proxies=build_proxies(proxy),
            )

            if response.status_code == 404:
                return 404
            if response.status_code == 429:
                raise requests.exceptions.RequestException(
                    f"HTTP 429 Too Many Requests cho SBD {sbd}"
                )

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError(f"Response không phải JSON object: {type(payload)}")

            if payload.get("success") is False:
                raise requests.exceptions.RequestException(
                    f"API trả success=false cho SBD {sbd}: {payload}"
                )

            rows = payload.get("data", [])
            total = payload.get("total", len(rows) if isinstance(rows, list) else 0)

            if isinstance(rows, list) and total > 0 and rows:
                return rows
            return 404

        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt >= max_attempts:
                break
            sleep_time = attempt + random.uniform(0.2, 1.2)
            if proxy:
                print(f"Lỗi {sbd} với proxy {proxy}: {e}. Retry...")
            else:
                print(f"Lỗi {sbd}: {e}. Retry...")
            time.sleep(sleep_time)

    return None


def save_rows_to_csv(rows: list[dict], output_csv: str, skip_existing: bool):
    saved = 0
    with write_lock:
        with Path(output_csv).open("a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            for item in rows:
                sbd = str(item.get("SOBAODANH", "")).strip()
                if skip_existing and sbd and sbd in EXISTING_SBD:
                    continue
                row = {field: item.get(field, "") for field in FIELDNAMES}
                writer.writerow(row)
                if sbd:
                    EXISTING_SBD.add(sbd)
                saved += 1
    return saved


def write_failed_sbd(path: str, failed: list[str]):
    with Path(path).open("w", encoding="utf-8") as f:
        for sbd in failed:
            f.write(sbd + "\n")


def worker(sbd: str, year: int, retries: int, timeout: int, output_csv: str, skip_existing: bool):
    result = get_score(sbd=sbd, year=year, retries=retries, timeout=timeout)

    if result == 404:
        print(f"404 / không có dữ liệu: {sbd}")
        return sbd, "not_found", 0

    if result is None:
        print(f"Vẫn lỗi: {sbd}")
        return sbd, "failed", 0

    saved = save_rows_to_csv(result, output_csv=output_csv, skip_existing=skip_existing)
    print(f"Đã xử lý lại thành công: {sbd} | ghi {saved} dòng")
    return sbd, "success", saved


def parse_args():
    parser = argparse.ArgumentParser(
        description="Xử lý lại các SBD bị lỗi được lưu trong errors.txt."
    )
    parser.add_argument("--error-file", default="errors.txt", help="File chứa SBD lỗi. Mặc định: errors.txt")
    parser.add_argument("--failed-file", default="errors_remaining.txt", help="File lưu SBD vẫn còn lỗi. Mặc định: errors_remaining.txt")
    parser.add_argument("--output-csv", default="diem_thi.csv", help="CSV đầu ra. Mặc định: diem_thi.csv")
    parser.add_argument("--proxy-file", default="proxies.txt", help="File proxy. Mặc định: proxies.txt")
    parser.add_argument("--year", type=int, default=2026, help="Năm tra cứu. Mặc định: 2026")
    parser.add_argument("--max-workers", type=int, default=10, help="Số luồng chạy đồng thời. Mặc định: 10")
    parser.add_argument("--retries", type=int, default=5, help="Số lần retry thêm cho mỗi SBD. Mặc định: 5")
    parser.add_argument("--timeout", type=int, default=12, help="Timeout mỗi request, đơn vị giây. Mặc định: 12")
    parser.add_argument("--skip-existing", action="store_true", help="Không ghi trùng SBD đã có trong CSV")
    parser.add_argument("--overwrite-errors", action="store_true", help="Ghi đè errors.txt bằng danh sách SBD vẫn còn lỗi")
    return parser.parse_args()


def main():
    global PROXIES, EXISTING_SBD

    args = parse_args()

    if args.max_workers < 1:
        raise ValueError("--max-workers phải >= 1")
    if args.retries < 0:
        raise ValueError("--retries phải >= 0")
    if args.timeout < 1:
        raise ValueError("--timeout phải >= 1")

    init_csv(args.output_csv)
    PROXIES = load_proxies(args.proxy_file)

    if PROXIES:
        print(f"Đã tải {len(PROXIES)} proxy từ {args.proxy_file}")
    else:
        print(f"Không có proxy trong {args.proxy_file}. Retry sẽ chạy không proxy.")

    if args.skip_existing:
        EXISTING_SBD = load_existing_sbd(args.output_csv)
        print(f"Đã đọc {len(EXISTING_SBD)} SBD có sẵn trong {args.output_csv}")

    sbd_list = read_error_sbd(args.error_file)
    if not sbd_list:
        print(f"Không có SBD lỗi nào trong {args.error_file}")
        write_failed_sbd(args.failed_file, [])
        return

    print(f"Bắt đầu xử lý lại {len(sbd_list)} SBD lỗi...")

    success_count = 0
    not_found_count = 0
    saved_rows = 0
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(
                worker,
                sbd,
                args.year,
                args.retries,
                args.timeout,
                args.output_csv,
                args.skip_existing,
            )
            for sbd in sbd_list
        ]

        for future in as_completed(futures):
            sbd, status, saved = future.result()
            if status == "success":
                success_count += 1
                saved_rows += saved
            elif status == "not_found":
                not_found_count += 1
            else:
                failed.append(sbd)

    write_failed_sbd(args.failed_file, failed)

    if args.overwrite_errors:
        write_failed_sbd(args.error_file, failed)
        print(f"Đã ghi đè {args.error_file} bằng danh sách còn lỗi.")

    print("=" * 60)
    print(f"Tổng SBD xử lý: {len(sbd_list)}")
    print(f"Thành công: {success_count}")
    print(f"Không có dữ liệu / 404: {not_found_count}")
    print(f"Vẫn lỗi: {len(failed)}")
    print(f"Số dòng đã ghi CSV: {saved_rows}")
    print(f"File SBD còn lỗi: {args.failed_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
