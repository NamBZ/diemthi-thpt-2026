import argparse
import csv
import queue
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter


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

OUTPUT_CSV = "diem_thi.csv"
ERROR_FILE = "errors.txt"
CHECKPOINT_FILE = "checkpoint.txt"
CONSECUTIVE_404_LIMIT = 100
CHECKPOINT_FLUSH_EVERY = 200
CSV_FLUSH_EVERY = 200
PROGRESS_EVERY = 1000

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

HOI_DONG_THI = {
    "01": "Thành phố Hà Nội",
    "04": "Tỉnh Cao Bằng",
    "08": "Tỉnh Tuyên Quang",
    "11": "Tỉnh Điện Biên",
    "12": "Tỉnh Lai Châu",
    "14": "Tỉnh Sơn La",
    "15": "Tỉnh Lào Cai",
    "19": "Tỉnh Thái Nguyên",
    "20": "Tỉnh Lạng Sơn",
    "22": "Tỉnh Quảng Ninh",
    "24": "Tỉnh Bắc Ninh",
    "25": "Tỉnh Phú Thọ",
    "31": "Thành phố Hải Phòng",
    "33": "Tỉnh Hưng Yên",
    "37": "Tỉnh Ninh Bình",
    "38": "Tỉnh Thanh Hóa",
    "40": "Tỉnh Nghệ An",
    "42": "Tỉnh Hà Tĩnh",
    "44": "Tỉnh Quảng Trị",
    "46": "Thành phố Huế",
    "48": "Thành phố Đà Nẵng",
    "51": "Tỉnh Quảng Ngãi",
    "52": "Tỉnh Gia Lai",
    "56": "Tỉnh Khánh Hòa",
    "66": "Tỉnh Đắk Lắk",
    "68": "Tỉnh Lâm Đồng",
    "75": "Tỉnh Đồng Nai",
    "79": "Thành phố Hồ Chí Minh",
    "80": "Tỉnh Tây Ninh",
    "82": "Tỉnh Đồng Tháp",
    "86": "Tỉnh Vĩnh Long",
    "91": "Tỉnh An Giang",
    "92": "Thành phố Cần Thơ",
    "96": "Tỉnh Cà Mau",
    "99": "Cục Quân huấn - Nhà trường, Bộ Quốc phòng",
}

write_lock = threading.Lock()
proxy_lock = threading.Lock()
checkpoint_lock = threading.Lock()
session_local = threading.local()

PROXIES: list[str] = []
CHECKPOINTS: dict[str, int] = {}
CSV_QUEUE: queue.Queue[list[dict[str, Any]] | None] = queue.Queue(maxsize=10000)
ERROR_QUEUE: queue.Queue[str | None] = queue.Queue(maxsize=10000)


def normalize_proxy(proxy: str) -> str:
    proxy = proxy.strip()
    if not proxy:
        return ""
    if not proxy.startswith(("http://", "https://", "socks4://", "socks5://")):
        proxy = f"http://{proxy}"
    return proxy


def load_proxies(proxy_file: str = "proxies.txt") -> list[str]:
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
    exclude = exclude or set()
    if not PROXIES:
        return None
    with proxy_lock:
        available_proxies = [proxy for proxy in PROXIES if proxy not in exclude]
        if not available_proxies:
            return None
        return random.choice(available_proxies)


def get_session():
    if not hasattr(session_local, "session"):
        session = requests.Session()
        session.headers.update(HEADERS)

        # Tăng pool để Session không phải mở lại kết nối quá nhiều.
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session_local.session = session
    return session_local.session


def format_sbd(ma_hoi_dong: str, suffix: int) -> str:
    return f"{ma_hoi_dong}{suffix:06d}"


def init_csv(output_csv: str):
    csv_path = Path(output_csv)
    if not csv_path.exists():
        with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def csv_writer(output_csv: str):
    init_csv(output_csv)
    buffer: list[dict[str, Any]] = []

    def flush():
        nonlocal buffer
        if not buffer:
            return
        with open(output_csv, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerows(buffer)
        buffer = []

    while True:
        item = CSV_QUEUE.get()
        if item is None:
            flush()
            CSV_QUEUE.task_done()
            break

        for row_item in item:
            row = {field: row_item.get(field, "") for field in FIELDNAMES}
            buffer.append(row)

        if len(buffer) >= CSV_FLUSH_EVERY:
            flush()

        CSV_QUEUE.task_done()


def error_writer(error_file: str):
    with open(error_file, "a", encoding="utf-8") as f:
        while True:
            sbd = ERROR_QUEUE.get()
            if sbd is None:
                ERROR_QUEUE.task_done()
                break
            f.write(sbd + "\n")
            ERROR_QUEUE.task_done()


def load_checkpoints(checkpoint_file: str):
    global CHECKPOINTS
    path = Path(checkpoint_file)
    checkpoints: dict[str, int] = {}

    if path.exists():
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if "=" not in line:
                    continue
                key, value = line.strip().split("=", 1)
                try:
                    checkpoints[key] = int(value)
                except ValueError:
                    checkpoints[key] = -1
        except OSError:
            pass

    with checkpoint_lock:
        CHECKPOINTS = checkpoints


def read_checkpoint(ma_hoi_dong: str) -> int:
    with checkpoint_lock:
        return CHECKPOINTS.get(ma_hoi_dong, -1)


def set_checkpoint(ma_hoi_dong: str, suffix: int):
    with checkpoint_lock:
        current = CHECKPOINTS.get(ma_hoi_dong, -1)
        if suffix > current:
            CHECKPOINTS[ma_hoi_dong] = suffix


def flush_checkpoints(checkpoint_file: str):
    with checkpoint_lock:
        content = "\n".join(
            f"{key}={value}"
            for key, value in sorted(CHECKPOINTS.items())
        )

    Path(checkpoint_file).write_text(content + "\n", encoding="utf-8")


def parse_tuoitre_payload(payload: Any):
    """
    Tuổi Trẻ thường trả JSON object dạng:
    {
        "success": true,
        "data": [ { "SOBAODANH": "92021838", ... } ],
        "total": 1
    }

    Trả về:
    - list row nếu có dữ liệu
    - 404 nếu data rỗng / total = 0
    """
    if not isinstance(payload, dict):
        raise ValueError(f"Response không phải JSON object: {type(payload)}")

    if payload.get("success") is False:
        raise requests.exceptions.RequestException(f"API trả success=false: {payload}")

    rows = payload.get("data", [])
    total = payload.get("total", len(rows) if isinstance(rows, list) else 0)

    if isinstance(rows, list) and total > 0 and rows:
        return rows

    return 404

def get_score(
    sbd: str,
    year: int = 2026,
    retries: int = 5,
    timeout: float = 6,
):
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
                timeout=(3.05, timeout),
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
            return parse_tuoitre_payload(payload)

        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt >= max_attempts:
                return None

            # Backoff ngắn hơn bản cũ để không bị ngủ quá lâu khi gặp lỗi hàng loạt.
            sleep_time = min(0.5 * attempt + random.uniform(0, 0.3), 2)
            print(f"Lỗi SBD {sbd}: {e}. Retry {attempt}/{retries}...")
            time.sleep(sleep_time)

    return None


def worker(ma_hoi_dong: str, suffix: int, year: int, retries: int, timeout: float):
    sbd = format_sbd(ma_hoi_dong, suffix)
    result = get_score(sbd, year=year, retries=retries, timeout=timeout)

    if result == 404:
        return suffix, "404", None

    if result is None:
        return suffix, "error", None

    return suffix, "data", result


def scan_hoi_dong(
    ma_hoi_dong: str,
    suffix_start: int,
    suffix_end: int,
    max_workers: int,
    consecutive_404_limit: int,
    checkpoint_file: str,
    year: int,
    retries: int,
    timeout: float,
    quiet: bool,
):
    ten_hoi_dong = HOI_DONG_THI.get(ma_hoi_dong, "Không rõ")

    print("=" * 70)
    print(f"Quét hội đồng: {ma_hoi_dong} - {ten_hoi_dong}")
    print(f"Dải SBD: {format_sbd(ma_hoi_dong, suffix_start)} -> {format_sbd(ma_hoi_dong, suffix_end)}")
    print(f"Năm: {year}")
    print(f"Tự dừng nếu {consecutive_404_limit} SBD liên tiếp trả 404")
    print("=" * 70)

    last_done = read_checkpoint(ma_hoi_dong)
    if last_done >= suffix_start:
        suffix_start = last_done + 1
        print(f"Tiếp tục từ: {format_sbd(ma_hoi_dong, suffix_start)}")

    if suffix_start > suffix_end:
        print(f"Hội đồng {ma_hoi_dong} đã quét xong.")
        return

    consecutive_404_count = 0
    processed_count = 0
    data_count = 0
    error_count = 0
    next_suffix_to_submit = suffix_start
    next_suffix_to_commit = suffix_start
    result_buffer: dict[int, tuple[str, list[dict[str, Any]] | None]] = {}
    stop_reason = ""

    def submit_one(executor: ThreadPoolExecutor, futures: dict):
        nonlocal next_suffix_to_submit
        if next_suffix_to_submit > suffix_end:
            return False
        suffix = next_suffix_to_submit
        future = executor.submit(worker, ma_hoi_dong, suffix, year, retries, timeout)
        futures[future] = suffix
        next_suffix_to_submit += 1
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[Any, int] = {}

        for _ in range(max_workers):
            if not submit_one(executor, futures):
                break

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)

            for future in done:
                suffix = futures.pop(future)
                try:
                    ret_suffix, status, rows = future.result()
                    result_buffer[ret_suffix] = (status, rows)
                except Exception as e:
                    sbd = format_sbd(ma_hoi_dong, suffix)
                    print(f"Lỗi worker {sbd}: {e}")
                    result_buffer[suffix] = ("error", None)

                # Chỉ submit tiếp khi chưa có lý do dừng.
                if not stop_reason:
                    submit_one(executor, futures)

            while next_suffix_to_commit in result_buffer:
                status, rows = result_buffer.pop(next_suffix_to_commit)
                sbd = format_sbd(ma_hoi_dong, next_suffix_to_commit)
                processed_count += 1

                if status == "404":
                    consecutive_404_count += 1
                else:
                    consecutive_404_count = 0

                if status == "data" and rows:
                    data_count += len(rows)
                    CSV_QUEUE.put(rows)
                    print(f"Có dữ liệu: {sbd}")

                elif status == "error":
                    error_count += 1
                    ERROR_QUEUE.put(sbd)
                    if not quiet:
                        print(f"Lỗi: {sbd}")

                elif not quiet and processed_count % PROGRESS_EVERY == 0:
                    print(
                        f"{ma_hoi_dong}: đã quét {processed_count:,} SBD, "
                        f"đến {sbd}, dữ liệu={data_count:,}, lỗi={error_count:,}, "
                        f"404 liên tiếp={consecutive_404_count}"
                    )

                set_checkpoint(ma_hoi_dong, next_suffix_to_commit)

                if processed_count % CHECKPOINT_FLUSH_EVERY == 0:
                    flush_checkpoints(checkpoint_file)

                if consecutive_404_count >= consecutive_404_limit:
                    stop_reason = (
                        f"Dừng hội đồng {ma_hoi_dong} - {ten_hoi_dong}: "
                        f"gặp {consecutive_404_limit} SBD 404 liên tiếp, kết thúc tại {sbd}."
                    )
                    break

                next_suffix_to_commit += 1

            if stop_reason:
                # Hủy các task chưa chạy. Task đang chạy sẽ tự xong rồi executor thoát.
                for future in list(futures):
                    future.cancel()
                break

    flush_checkpoints(checkpoint_file)

    if stop_reason:
        print(stop_reason)
    else:
        print(
            f"Xong hội đồng {ma_hoi_dong}: quét {processed_count:,} SBD, "
            f"ghi {data_count:,} dòng dữ liệu, lỗi {error_count:,}."
        )


def scan_hoi_dong_task(
    ma_hoi_dong: str,
    suffix_start: int,
    suffix_end: int,
    max_workers: int,
    consecutive_404_limit: int,
    checkpoint_file: str,
    year: int,
    retries: int,
    timeout: float,
    quiet: bool,
):
    try:
        scan_hoi_dong(
            ma_hoi_dong=ma_hoi_dong,
            suffix_start=suffix_start,
            suffix_end=suffix_end,
            max_workers=max_workers,
            consecutive_404_limit=consecutive_404_limit,
            checkpoint_file=checkpoint_file,
            year=year,
            retries=retries,
            timeout=timeout,
            quiet=quiet,
        )
    except Exception as e:
        print(f"Lỗi hội đồng {ma_hoi_dong}: {e}")


def scan_multi_hoi_dong(
    hoi_dong_list: list[str],
    suffix_start: int,
    suffix_end: int,
    max_workers: int,
    hoi_dong_workers: int,
    consecutive_404_limit: int,
    checkpoint_file: str,
    year: int,
    retries: int,
    timeout: float,
    quiet: bool,
):
    if not hoi_dong_list:
        print("Không có hội đồng nào để quét.")
        return

    hoi_dong_workers = max(1, min(hoi_dong_workers, len(hoi_dong_list)))
    total_request_workers = hoi_dong_workers * max_workers

    print(
        f"Chạy song song {hoi_dong_workers}/{len(hoi_dong_list)} hội đồng. "
        f"Tổng request worker tối đa: {total_request_workers}."
    )

    with ThreadPoolExecutor(max_workers=hoi_dong_workers) as executor:
        futures = [
            executor.submit(
                scan_hoi_dong_task,
                ma_hoi_dong,
                suffix_start,
                suffix_end,
                max_workers,
                consecutive_404_limit,
                checkpoint_file,
                year,
                retries,
                timeout,
                quiet,
            )
            for ma_hoi_dong in hoi_dong_list
        ]

        for future in futures:
            future.result()


def parse_hoi_dong(value: str):
    value = value.strip()
    if value.lower() == "all":
        return list(HOI_DONG_THI.keys())

    codes: list[str] = []
    for item in value.split(","):
        code = item.strip()
        if not code:
            continue
        code = code.zfill(2)
        if code not in HOI_DONG_THI:
            raise ValueError(f"Mã hội đồng thi không hợp lệ: {code}")
        codes.append(code)
    return codes


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hoi-dong", required=True, help="Ví dụ: 01 hoặc 01,04,79 hoặc all")
    parser.add_argument("--suffix-start", type=int, default=0, help="Mặc định: 0")
    parser.add_argument("--suffix-end", type=int, default=999999, help="Mặc định: 999999")
    parser.add_argument("--max-workers", type=int, default=30, help="Số request worker mỗi hội đồng. Mặc định: 20")
    parser.add_argument("--hoi-dong-workers", type=int, default=5, help="Số hội đồng chạy song song. Mặc định: 2")
    parser.add_argument("--proxy-file", type=str, default="proxies.txt", help="Mặc định: proxies.txt")
    parser.add_argument("--output", type=str, default=OUTPUT_CSV, help=f"Mặc định: {OUTPUT_CSV}")
    parser.add_argument("--error-file", type=str, default=ERROR_FILE, help=f"Mặc định: {ERROR_FILE}")
    parser.add_argument("--checkpoint-file", type=str, default=CHECKPOINT_FILE, help=f"Mặc định: {CHECKPOINT_FILE}")
    parser.add_argument("--consecutive-404-limit", type=int, default=CONSECUTIVE_404_LIMIT, help="Mặc định: 100")
    parser.add_argument("--year", type=int, default=2026, help="Năm tra cứu điểm thi. Mặc định: 2026")
    parser.add_argument("--retries", type=int, default=5, help="Retry khi lỗi request/JSON. Mặc định: 2")
    parser.add_argument("--timeout", type=float, default=6, help="Read timeout mỗi request, giây. Mặc định: 6")
    parser.add_argument("--quiet", action="store_true", help="Giảm log 404/lỗi để chạy nhẹ hơn")
    return parser.parse_args()


def main():
    global PROXIES
    args = parse_args()

    if args.suffix_start < 0 or args.suffix_start > 999999:
        raise ValueError("--suffix-start phải nằm trong khoảng 0 -> 999999")
    if args.suffix_end < 0 or args.suffix_end > 999999:
        raise ValueError("--suffix-end phải nằm trong khoảng 0 -> 999999")
    if args.suffix_start > args.suffix_end:
        raise ValueError("--suffix-start không được lớn hơn --suffix-end")
    if args.max_workers < 1:
        raise ValueError("--max-workers phải >= 1")
    if args.hoi_dong_workers < 1:
        raise ValueError("--hoi-dong-workers phải >= 1")
    if args.consecutive_404_limit < 1:
        raise ValueError("--consecutive-404-limit phải >= 1")

    init_csv(args.output)
    load_checkpoints(args.checkpoint_file)
    PROXIES = load_proxies(args.proxy_file)

    if PROXIES:
        print(f"Đã tải {len(PROXIES)} proxy từ {args.proxy_file}")
    else:
        print(f"Không có proxy trong {args.proxy_file}. Sẽ chạy request trực tiếp.")

    csv_thread = threading.Thread(target=csv_writer, args=(args.output,), daemon=True)
    error_thread = threading.Thread(target=error_writer, args=(args.error_file,), daemon=True)
    csv_thread.start()
    error_thread.start()

    try:
        hoi_dong_list = parse_hoi_dong(args.hoi_dong)
        scan_multi_hoi_dong(
            hoi_dong_list=hoi_dong_list,
            suffix_start=args.suffix_start,
            suffix_end=args.suffix_end,
            max_workers=args.max_workers,
            hoi_dong_workers=args.hoi_dong_workers,
            consecutive_404_limit=args.consecutive_404_limit,
            checkpoint_file=args.checkpoint_file,
            year=args.year,
            retries=args.retries,
            timeout=args.timeout,
            quiet=args.quiet,
        )
    finally:
        flush_checkpoints(args.checkpoint_file)
        CSV_QUEUE.put(None)
        ERROR_QUEUE.put(None)
        CSV_QUEUE.join()
        ERROR_QUEUE.join()
        csv_thread.join(timeout=5)
        error_thread.join(timeout=5)


if __name__ == "__main__":
    main()
