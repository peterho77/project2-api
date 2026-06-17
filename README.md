# Tiki Async Data Scraper

![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)
![Poetry](https://img.shields.io/badge/poetry-package%20manager-cyan)
![Asyncio](https://img.shields.io/badge/asyncio-aiohttp-green)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows-lightgrey)

Tiki Async Data Scraper là một hệ thống thu thập dữ liệu tự động (ETL Pipeline) hiệu năng cao. Dự án được thiết kế với kiến trúc bất đồng bộ (Asynchronous I/O) để xử lý lượng lớn dữ liệu đầu vào, kết hợp với các cơ chế tự động phục hồi lỗi, chống trùng lặp và ghi đè an toàn (Atomic Write).

## ✨ Tính năng cốt lõi (Core Features)

* **Hiệu suất cao (High Concurrency):** Sử dụng `aiohttp` và `asyncio` kết hợp `uvloop` (trên Linux) để tối đa hóa băng thông mạng và luồng xử lý đồng thời, cho phép cào hàng trăm ngàn ID sản phẩm trong thời gian ngắn.
* **Kiến trúc Đa giai đoạn (Multi-Phase Pipeline):**
  * **Phase 1 (Cào mới):** Xử lý quét toàn bộ lượng ID đầu vào chưa từng được thao tác.
  * **Phase 2 (Retry):** Tự động phân loại và cào lại các ID bị lỗi mạng/timeout mà không làm ảnh hưởng đến dữ liệu đã thành công.
* **Bảo vệ toàn vẹn dữ liệu (Data Integrity):**
  * Ứng dụng lý thuyết Tập hợp (Set Theory) và Biểu thức chính quy (Regex) để làm sạch dữ liệu rác (null, chuỗi dính liền) và chống trùng lặp ID (100% Unique).
  * Cơ chế **Atomic Write**: Sử dụng file tạm (`.tmp`) để đảm bảo không thất thoát dữ liệu ngay cả khi hệ thống sập nguồn đột ngột.
* **Cơ chế chịu lỗi (Fail-Safe & Recovery):** * Tích hợp Lock File (`running.lock`) để kiểm soát tiến trình độc quyền, chống chạy đè.
  * Quản lý tín hiệu an toàn (SIGTERM handling) để luồng tự dọn dẹp bộ nhớ trước khi bị hệ thống ngắt.
* **Hệ thống Log chuyên sâu:** Phân tách rõ ràng giữa log chi tiết (`detail.log`), log tiến độ (`progress.log`) và bảng tổng hợp (`summary.log`).

## 🛠 Thư viện & Công nghệ sử dụng

* **Ngôn ngữ:** Python 3.13+
* **Quản lý Package & Môi trường:** [Poetry](https://python-poetry.org/)
* **Thư viện chính:**
  * `aiohttp`: Quản lý HTTP Client session và Connection Pooling.
  * `pandas`: Hỗ trợ chuẩn bị và trích xuất dữ liệu mảng (ETL) từ file CSV.
  * `uvloop`: Tối ưu hóa Event Loop (Nhanh hơn gấp 2-4 lần asyncio mặc định).
* **Triển khai:** Systemd Service (Linux Background Process).

## 🚀 Cài đặt & Môi trường

Dự án sử dụng **Poetry** để quản lý môi trường ảo và các dependency.

**Bước 1: Clone dự án và di chuyển vào thư mục gốc**
  ```bash
  git clone <your-repo-url>
  cd project2-api
  ```

**Bước 2: Cài đặt các thư viện phụ thuộc bằng Poetry**

  ```bash
  poetry install
  ```

**Bước 3: Kích hoạt môi trường ảo (Virtual Environment)**

  ```bash
  poetry shell
  ```

_(Lưu ý: Môi trường ảo .venv phải được trỏ đúng vào Python >= 3.13)_

## ⚙️ Cấu hình (Configuration)

Các thông số hệ thống được quản lý thông qua file config (VD: config/settings.py hoặc config.yaml). Các thông số quan trọng cần lưu ý:

* **CONCURRENCY_LIMIT**: Giới hạn số lượng worker chạy đồng thời. Khuyến nghị đặt ở mức 10-20 nếu chạy trên laptop cá nhân để tránh quá tải nhiệt, và 50-100 nếu chạy trên server.

* **CSV_FILE_PATTERN**: Regex để nhận diện các file input.

* **CSV_COLUMN_NAME**: Tên cột chứa ID sản phẩm cần quét.

## 💻 Hướng dẫn sử dụng (Usage)

**Cách 1**: Chạy trực tiếp qua Command Line (Dev Mode)
Chạy script chính thông qua trình thông dịch của môi trường ảo:

```bash
python main.py
```

_Lưu ý: Nhấn Ctrl + C để kích hoạt cơ chế ngắt an toàn (Graceful Shutdown). Hệ thống sẽ tự động tổng hợp lỗi và dọn dẹp file trước khi thoát._


**Cách 2:** Chạy nền qua Systemd (Production Mode trên Linux)
Để tiến trình hoạt động độc lập không phụ thuộc vào Terminal, dự án được cấu hình sẵn dưới dạng service:

Bắt đầu tiến trình:

```bash
sudo systemctl start tiki_scraper.service
```

Kiểm tra trạng thái & Log lỗi:

```bash
sudo systemctl status tiki_scraper.service
sudo journalctl -u tiki_scraper.service -n 50 -f
```

Dừng tiến trình (Kích hoạt Graceful Shutdown):

```bash
sudo systemctl stop tiki_scraper.service
```

📂 Kiến trúc thư mục cơ bản

```text
project2-api/
├── config/
│   ├──.env                  # Chứa các biến môi trường (API Keys, Passwords)
│   ├── config.yaml          # Chứa thông số cấu hình hệ thống (Limit, File pattern, CSV column)
│   └── settings.py          # Load và parse dữ liệu từ .env và config.yaml để cấp cho ứng dụng
├── data/
│   ├── input/               # Nơi đặt các file CSV thô chứa ID cần cào (VD: products1.csv)
│   └── output/              # Thư mục chứa kết quả và checkpoint bảo vệ tiến trình
│       ├── success_ids.csv  # Lưu các ID đã cào thành công
│       ├── failed_ids.csv   # Lưu các ID lỗi mạng/timeout (Sẽ được Retry)
│       ├── dead_ids.csv     # Lưu các ID đã bị hệ thống xóa vĩnh viễn (Lỗi 404)
│       ├── summary.log      # Bảng sao kê tổng kết số lượng sau mỗi phiên chạy
│       ├── detail.log       # Log truy vết các lỗi Crash/Exception sâu của hệ thống
│       └── progress.log     # Log tiến độ realtime
├── src/
│   ├── __init__.py
│   ├── api_client.py        # Chứa logic Request HTTP, xử lý Timeout & Connection Pool
│   ├── json_handler.py      # Module thao tác Ghi/Đọc file (File Writer ẩn danh)
│   └── utils.py             # Bộ công cụ Helper: Cleanup Logs, Regex Parser, Lock File
├── tests/
│   ├── __init__.py          # Khai báo biến thư mục thành module
│   ├── conftest.py          # Chứa các Fixture dùng chung cho mọi file test
│   ├── test_utils.py        # Test các hàm logic Regex, Set Theory, File lock
│   └── test_api_client.py   # Test luồng gọi API bất đồng bộ
└── .github/
│   └── workflows/
│       └── ci.yml           # File cấu hình CI/CD chạy tự động
├── pytest.ini               # Cấu hình môi trường cho Pytest
├── main.py                  # Orchestrator trung tâm - Điều phối Queue, Worker và Phases
├── poetry.lock              # Khóa phiên bản tuyệt đối của toàn bộ Dependency
├── pyproject.toml           # File manifest của Poetry (Khai báo package, tác giả)
└── README.md                # Tài liệu kỹ thuật dự án
```

👤 Tác giả
Steven Ho - BE
