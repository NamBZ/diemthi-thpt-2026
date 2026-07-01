# Tool Crawl Điểm Thi THPT

Công cụ tự động thu thập (crawl) dữ liệu điểm thi tốt nghiệp THPT Quốc Gia từ các trang báo điện tử (Tuổi Trẻ, Thanh Niên). Công cụ được viết bằng Python, hỗ trợ chạy đa luồng (multithreading), tự động thay đổi proxy và có cơ chế tiếp tục từ checkpoint để tối ưu tốc độ và không bị mất tiến trình khi gián đoạn.

Download dữ liệu điểm thi từ báo Tuổi Trẻ (2026):
- https://www.mediafire.com/file/1zicb03hkxlwzr7/diem_thi_tuoitre.csv/file

Download dữ liệu điểm thi từ báo Thanh Niên (2026):
- https://www.mediafire.com/file/vkqaoxwv4qoys9h/diem_thi_thanhnien.csv/file

## Các tính năng chính
- **Tra cứu theo hội đồng:** Hỗ trợ quét điểm thi theo từng mã hội đồng thi (ví dụ: Hà Nội `01`, TP.HCM `79`).
- **Đa luồng hiệu suất cao:** Hỗ trợ chạy đồng thời nhiều hội đồng thi cùng lúc với số lượng worker tùy chỉnh.
- **Vượt giới hạn kết nối (Bypass Rate Limit):** Hỗ trợ sử dụng proxy (HTTP, SOCKS4, SOCKS5) xoay vòng tự động để tránh bị chặn IP.
- **Cơ chế Checkpoint:** Tự động lưu lại tiến trình đã quét (checkpoint) để có thể tiếp tục ngay tại vị trí bị dừng đột ngột mà không cần quét lại từ đầu.
- **Xuất file chuẩn:** Xuất dữ liệu trực tiếp ra file `.csv` thuận tiện cho việc phân tích.
- **Script sửa lỗi riêng biệt:** Tích hợp các script (`fix_*.py`) để quét lại cụ thể các số báo danh bị lỗi hoặc bị thiếu.

## Cài đặt

1. Đảm bảo bạn đã cài đặt **Python 3**.
2. Cài đặt các thư viện yêu cầu (sử dụng `pip`):
   ```bash
   pip install requests
   ```
3. Tạo file `proxies.txt` trong cùng thư mục (nếu cần dùng proxy) với mỗi proxy trên một dòng theo định dạng `http://ip:port`, `socks4://ip:port`, hoặc `socks5://ip:port`.

## Cách sử dụng

### 1. Quét từ báo Tuổi Trẻ
Sử dụng script `diem_tuoitre.py`.

```bash
python diem_tuoitre.py --hoi-dong 01,79
```

**Các tham số dòng lệnh:**
- `--hoi-dong`: (Bắt buộc) Mã hội đồng thi cần quét. Có thể ghi nhiều mã cách nhau bằng dấu phẩy (vd: `01,04,79`) hoặc `all` để quét toàn quốc.
- `--suffix-start`: Hậu tố số báo danh bắt đầu (mặc định: `0`).
- `--suffix-end`: Hậu tố số báo danh kết thúc (mặc định: `999999`).
- `--max-workers`: Số luồng quét cho mỗi hội đồng (mặc định: `30`).
- `--hoi-dong-workers`: Số hội đồng quét song song (mặc định: `5`).
- `--proxy-file`: Đường dẫn file proxy (mặc định: `proxies.txt`).
- `--output`: File xuất kết quả (mặc định: `diem_thi.csv`).
- `--year`: Năm thi cần lấy điểm (mặc định: `2026`).

### 2. Quét từ báo Thanh Niên
Sử dụng script `diem_thanhnien.py` với cấu trúc tham số tương tự.

```bash
python diem_thanhnien.py --hoi-dong all --max-workers 20
```

### 3. Khắc phục sự cố và quét lại (Fix Scripts)
Sau khi quét xong toàn bộ, nếu kiểm tra thấy thiếu dữ liệu hoặc bị lỗi mạng liên tục với một số SBD, hãy sử dụng các script sửa lỗi:
- `fix_tuoitre.py`
- `fix_thanhnien.py`

Các script này thường đọc từ file log báo lỗi (`errors.txt`) để tự động thử lại đúng những số báo danh đó.

## Cấu trúc File Đầu Ra (`diem_thi.csv`)
File CSV sẽ có cấu trúc gồm các cột như:
`STT, SOBAODANH, TOAN, VA, LI, HO, SI, SU, DI, KTPL, TI, CNCN, CNNN, NN, MON_NN, NGAY_SINH, file_name`

## Lưu ý Quan Trọng
- **Sử dụng Proxy:** Khi quét lượng lớn số báo danh và sử dụng `--max-workers` cao, hệ thống nguồn có thể chặn IP của bạn. Lúc này, proxy (`proxies.txt`) là bắt buộc để duy trì tốc độ.
- **Dữ liệu lưu tự động:** Dữ liệu điểm thi và checkpoint được script ghi (`flush`) liên tục. Do đó, bạn có thể ngắt script (`Ctrl + C`) bất cứ lúc nào một cách an toàn. Lần chạy sau với lệnh tương tự, tool sẽ tự động đọc `checkpoint.txt` để tiếp tục.
