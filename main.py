import sys
import time
import re
import signal
import asyncio
import pandas as pd
import aiohttp
import uvloop
from pathlib import Path
from typing import List, Dict   

# ---------------------------------------------------------
# TÍCH HỢP HỆ THỐNG TỪ CÁC MODULE
# ---------------------------------------------------------
from src.api_client import fetch_product_info
from src.json_handler import file_writer
from src.utils import (
    ensure_directory_exists,
    create_file_logger,
    get_handled_ids,
    cleanup_log_files,
    check_and_recover_lock,
    progress_reporter
)

# ==========================================
# 1. KHỞI TẠO ĐƯỜNG DẪN & CẤU HÌNH CƠ BẢN
# ==========================================
from config.settings import config, INPUT_DIR, OUTPUT_DIR

ensure_directory_exists(INPUT_DIR)
ensure_directory_exists(OUTPUT_DIR)

# Khởi tạo Loggers (Factory Pattern)
detail_logger = create_file_logger("detail", OUTPUT_DIR / "detail.log", '%(asctime)s - %(levelname)s - %(message)s')
summary_logger = create_file_logger("summary", OUTPUT_DIR / "summary.log", '\n[%(asctime)s] %(message)s',
                                    '%d/%m/%Y %H:%M:%S')
progress_logger = create_file_logger("progress", OUTPUT_DIR / "progress.log", '%(asctime)s - %(message)s',
                                     '%d/%m/%Y %H:%M:%S')


# ==========================================
# 2. CUSTOM EXCEPTIONS CHO PIPELINE
# ==========================================
class PipelineError(Exception):
    """Lỗi tổng quát cho toàn bộ vòng đời Pipeline."""
    pass


class DataPreparationError(PipelineError):
    """Lỗi xảy ra trong quá trình quét, đọc và chuẩn bị dữ liệu đầu vào."""
    pass


class ConfigurationError(PipelineError):
    """Lỗi do file cấu hình sai định dạng hoặc thiếu trường dữ liệu."""
    pass


# ==========================================
# 3. HELPER FUNCTIONS (Chuẩn bị dữ liệu - SRP & Pure-like)
# ==========================================
def _get_target_files(input_dir: Path, pattern_str: str, config_limit: int) -> List[Path]:
    """Quét thư mục, lọc bằng Regex và xác định số lượng file cần chạy."""
    assert input_dir.exists(), f"Thư mục {input_dir} không tồn tại."
    assert isinstance(pattern_str, str) and pattern_str, "Regex pattern không hợp lệ."

    pattern = re.compile(pattern_str)
    matched_files = [f for f in input_dir.iterdir() if f.is_file() and pattern.match(f.name)]

    if not matched_files:
        raise DataPreparationError(f"Không tìm thấy file nào khớp regex '{pattern_str}' trong {input_dir}")

    # Sắp xếp file theo số tự nhiên trong tên (VD: products2.csv đứng trước products10.csv)
    def extract_number(filepath: Path) -> int:
        match = re.search(r'\d+', filepath.name)
        return int(match.group()) if match else 0

    matched_files.sort(key=extract_number)

    # Xác định giới hạn file
    limit = len(matched_files)
    if sys.stdin.isatty():
        try:
            user_input = input(f"[?] Tìm thấy {limit} file. Nhập số file muốn chạy (Nhấn Enter để chạy TẤT CẢ): ")
            if user_input.strip():
                val = int(user_input)
                if val <= 0:
                    raise DataPreparationError("Số lượng file nhập vào phải lớn hơn 0.")
                limit = val
        except ValueError:
            raise DataPreparationError("Đầu vào không phải là số nguyên hợp lệ.")
    elif config_limit > 0:
        limit = config_limit

    return matched_files[:limit]


def _extract_ids_from_csvs(files: List[Path], column_name: str) -> List[str]:
    """Đọc dữ liệu từ danh sách file CSV và bóc tách ID. Bắt lỗi pandas an toàn."""
    assert files, "Danh sách file cần đọc đang trống."
    assert column_name, "Tên cột (column_name) chưa được cấu hình."

    extracted_ids = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            if column_name not in df.columns:
                detail_logger.warning(f"Bỏ qua {file_path.name}: Không tìm thấy cột '{column_name}'.")
                continue

            # Ép kiểu an toàn (float -> int -> str) để chống nhiễu dữ liệu
            ids = df[column_name].dropna().apply(lambda x: str(int(float(x)))).tolist()
            extracted_ids.extend(ids)

        except pd.errors.EmptyDataError:
            detail_logger.warning(f"Bỏ qua {file_path.name}: File CSV trống.")
        except Exception as e:
            raise DataPreparationError(f"Lỗi không lường trước khi đọc file {file_path.name}: {e}") from e

    # Lọc trùng lặp mà vẫn giữ nguyên thứ tự (Fastest way in Python)
    unique_ids = list(dict.fromkeys(extracted_ids))

    if not unique_ids:
        raise DataPreparationError("Không trích xuất được bất kỳ ID hợp lệ nào từ các file CSV.")

    return unique_ids


# ==========================================
# 4. CORE ASYNC WORKERS
# ==========================================
async def worker(
        worker_id: int,
        job_queue: asyncio.Queue,
        result_queue: asyncio.Queue,
        session: aiohttp.ClientSession,
        error_summary: Dict[str, int],
        progress_dict: Dict[str, int]
) -> None:
    """Worker đa luồng xử lý I/O Network."""
    assert isinstance(worker_id, int), "worker_id phải là số nguyên"

    while True:
        # ==========================================
        # BƯỚC 1: NHẬN VIỆC
        # ==========================================
        try:
            product_id = await job_queue.get()
        except asyncio.CancelledError:
            # Bị hệ thống hủy khi đang đứng chờ rảnh rỗi -> Rút lui luôn, KHÔNG chạy finally
            break

        # ==========================================
        # BƯỚC 2: XỬ LÝ VIỆC (Đã lấy được việc thì BẮT BUỘC phải có task_done)
        # ==========================================
        try:
            # Khối try chính gọi API
            result = await fetch_product_info(session, product_id)

            if result and isinstance(result, dict) and result.get("error"):
                status = str(result.get("status_code", "UNKNOWN"))
                error_summary[status] = error_summary.get(status, 0) + 1
                await result_queue.put((product_id, None))
            else:
                await result_queue.put((product_id, result))

        except asyncio.CancelledError:
            # Bị hủy ngang khi đang ráng gọi API -> Nghỉ, nhưng phần finally vẫn sẽ quét dọn ID này
            break

        except Exception as e:
            # Bare exception lưới an toàn
            detail_logger.error(f"Lỗi crash ở worker {worker_id} ID {product_id}: {e}")
            error_summary["FATAL_ERROR"] = error_summary.get("FATAL_ERROR", 0) + 1
            await result_queue.put((product_id, None))

        finally:
            progress_dict["done"] = progress_dict.get("done", 0) + 1
            job_queue.task_done()


# ==========================================
# 5. ENGINE ORCHESTRATOR
# ==========================================
async def run_engine(
        ids_list: List[str],
        phase_name: str,
        session: aiohttp.ClientSession,
        error_summary: Dict[str, int],
        concurrency_limit: int
) -> None:
    """Quản lý vòng đời của Queues và các Workers cho 1 giai đoạn."""
    if not ids_list:
        print(f"[*] {phase_name}: Không có ID nào cần chạy.")
        return

    assert concurrency_limit > 0, "Concurrency limit phải lớn hơn 0"

    total_ids = len(ids_list)
    print(f"[*] Bắt đầu {phase_name} với {total_ids} ID...")

    job_queue: asyncio.Queue = asyncio.Queue()
    result_queue: asyncio.Queue = asyncio.Queue()
    progress_dict = {"done": 0}

    # Nạp dữ liệu vào Queue
    for pid in ids_list:
        job_queue.put_nowait(pid)

    writer_task = asyncio.create_task(file_writer(result_queue))
    reporter_task = asyncio.create_task(progress_reporter(progress_dict, total_ids, phase_name, progress_logger))

    workers = [
        asyncio.create_task(worker(i, job_queue, result_queue, session, error_summary, progress_dict))
        for i in range(concurrency_limit)
    ]

    try:
        await job_queue.join()
        await result_queue.join()

        # Gửi Poison Pill an toàn vào luồng ghi
        await result_queue.put(None)
        await writer_task

        sys.stdout.write(f"\r[>] Tiến độ: {total_ids}/{total_ids} ID (100.00%) - HOÀN TẤT!          \n")
        sys.stdout.flush()

    except asyncio.CancelledError:
        print(f"\n[!] Bị ngắt! Đang dọn dẹp {phase_name}...")
        raise

    finally:
        # Hủy các task background để dọn sạch Event Loop
        reporter_task.cancel()
        for w in workers:
            if not w.done():
                w.cancel()
        if not writer_task.done():
            writer_task.cancel()


# ==========================================
# 6. PIPELINE ORCHESTRATOR
# ==========================================
async def process_pipeline() -> None:
    """Điều phối toàn bộ quy trình: Đọc -> Lọc -> Chạy Phase 1 -> Chạy Phase 2 -> Dọn dẹp."""

    # 1. Nạp và Kiểm tra Cấu hình (Fail Fast)
    file_pattern = config.get("CSV_FILE_PATTERN")
    column_name = config.get("CSV_COLUMN_NAME")
    success_file = config.get("SUCCESS_FILE", "success.csv")
    failed_file = config.get("FAILED_FILE", "failed.csv")
    dead_file = config.get("DEAD_FILE", "dead.csv")
    config_limit = int(config.get("NUM_FILES_TO_RUN", 0))
    concurrency_limit = int(config.get("CONCURRENCY_LIMIT", 50))

    if not file_pattern or not column_name:
        raise ConfigurationError("Thiếu biến CSV_FILE_PATTERN hoặc CSV_COLUMN_NAME trong config.yaml")

    # 2. Chuẩn bị Dữ Liệu
    selected_files = _get_target_files(INPUT_DIR, file_pattern, config_limit)
    print(f"[*] Đã chọn {len(selected_files)} file: {[f.name for f in selected_files]}")

    all_product_ids = _extract_ids_from_csvs(selected_files, column_name)

    # 3. Phân luồng ID
    processed_ids = get_handled_ids([success_file, failed_file, dead_file], OUTPUT_DIR)
    new_ids = [pid for pid in all_product_ids if pid not in processed_ids]

    print(f"\n--- GIAI ĐOẠN 1: CÀO DỮ LIỆU MỚI ---")
    print(
        f"Tổng số ID đầu vào: {len(all_product_ids)} | Đã xử lý cũ: {len(processed_ids)} | Cần cào mới: {len(new_ids)}\n")

    connector = aiohttp.TCPConnector(limit=concurrency_limit, limit_per_host=0, force_close=False, ttl_dns_cache=600)
    error_summary: Dict[str, int] = {}

    # 4. Thực thi Network Loop
    async with aiohttp.ClientSession(connector=connector) as session:

        # --- PHASE 1: CÀO DATA MỚI ---
        if new_ids:
            print(f"--- GIAI ĐOẠN 1: CÀO DỮ LIỆU MỚI ---")
            await run_engine(new_ids, "GIAI ĐOẠN 1 (Data Mới)", session, error_summary, concurrency_limit)
        else:
            print("[*] Toàn bộ dữ liệu đã được quét. BỎ QUA GIAI ĐOẠN 1.")

        # --- PHASE 2: RETRY ---
        print(f"\n--- GIAI ĐOẠN 2: CHẠY LẠI CÁC ID LỖI (RETRY) ---")
        updated_success_ids = get_handled_ids([success_file], OUTPUT_DIR)
        updated_failed_ids = get_handled_ids([failed_file], OUTPUT_DIR)
        updated_dead_ids = get_handled_ids([dead_file], OUTPUT_DIR)

        retry_ids = [
            pid for pid in updated_failed_ids
            if pid not in updated_success_ids and pid not in updated_dead_ids
        ]
        await run_engine(retry_ids, "GIAI ĐOẠN 2 (Data Lỗi)", session, error_summary, concurrency_limit)

    # 5. Dọn dẹp File Cuối Cùng
    cleanup_log_files(OUTPUT_DIR / success_file, OUTPUT_DIR / failed_file, OUTPUT_DIR / dead_file)

    # 6. Report Tổng Kết
    print("\n[====== HOÀN TẤT TOÀN BỘ TIẾN TRÌNH ======]")
    summary_logger.info("========= BẢNG SAO KÊ LỖI (REPORT) =========")
    if not error_summary:
        print("[+] Tuyệt vời! Hoàn thành 100% không lỗi.")
        summary_logger.info("Tuyệt vời! Hoàn thành 100% không lỗi.")
    else:
        for status, count in error_summary.items():
            print(f"  -> Lỗi HTTP/Hệ thống {status}: {count} ID")
            summary_logger.info(f"-> Lỗi HTTP/Hệ thống {status}: {count} ID")
    print("============================================\n")


# ==========================================
# 7. ENTRY POINT & SIGNAL HANDLING
# ==========================================
def handle_sigterm(*args):
    """Chuyển đổi tín hiệu hệ thống thành Exception để code tự gọi khối Finally."""
    raise KeyboardInterrupt("Nhận tín hiệu SIGTERM từ hệ thống.")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)

    if sys.platform != 'win32':
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    else:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    lock_file = OUTPUT_DIR / "running.lock"
    summary_log_file = OUTPUT_DIR / "summary.log"

    # Kiểm tra khôi phục Crash
    check_and_recover_lock(lock_file, summary_log_file, summary_logger)

    # Ghi Lock mới
    start_time = time.time()
    try:
        with open(lock_file, 'w', encoding='utf-8') as f:
            f.write(str(start_time))
    except OSError as e:
        print(f"[!] KHÔNG THỂ KHỞI TẠO TIẾN TRÌNH: Lỗi ghi file lock ({e})")
        sys.exit(1)

    summary_logger.info("============================================")
    summary_logger.info("BẮT ĐẦU PHIÊN CHẠY MỚI")

    status_msg = "CHƯA XÁC ĐỊNH"
    try:
        asyncio.run(process_pipeline())
        status_msg = "KẾT THÚC THÀNH CÔNG"

    except KeyboardInterrupt:
        status_msg = "KẾT THÚC DO NGƯỜI DÙNG/HỆ THỐNG DỪNG"
        print(f"\n[!] CẢNH BÁO: {status_msg}")

    except ConfigurationError as ce:
        status_msg = f"KẾT THÚC DO SAI CẤU HÌNH: {ce}"
        print(f"\n[!] LỖI CẤU HÌNH: {status_msg}")

    except DataPreparationError as dpe:
        status_msg = f"KẾT THÚC DO LỖI DỮ LIỆU ĐẦU VÀO: {dpe}"
        print(f"\n[!] LỖI DỮ LIỆU: {status_msg}")

    except Exception as e:
        status_msg = f"KẾT THÚC DO LỖI CRASH KHÔNG LƯỜNG TRƯỚC: {e}"
        print(f"\n[!] LỖI NGHIÊM TRỌNG: {status_msg}")
        summary_logger.exception("Chi tiết Stacktrace lỗi:")  # Bổ sung stacktrace vào log

    finally:
        end_time = time.time()
        elapsed_seconds = end_time - start_time
        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        print(f"\n[*] Thời gian phiên chạy này: {time_str}")
        summary_logger.info(f"Tình trạng: {status_msg}")
        summary_logger.info(f"Tổng thời gian chạy: {time_str}")
        summary_logger.info("============================================\n")

        try:
            success_file = config.get("SUCCESS_FILE", "success.csv")
            failed_file = config.get("FAILED_FILE", "failed.csv")
            dead_file = config.get("DEAD_FILE", "dead.csv")

            cleanup_log_files(OUTPUT_DIR / success_file, OUTPUT_DIR / failed_file, OUTPUT_DIR / dead_file)
        except Exception as e:
            print(f"[!] Lỗi khi dọn dẹp file log cuối phiên: {e}")
            summary_logger.error(f"Lỗi dọn dẹp file: {e}")
        # =========================================================

        # Thu dọn lock file an toàn
        try:
            if lock_file.exists():
                lock_file.unlink()
        except OSError as e:
            summary_logger.error(f"Không thể xóa file lock sau khi kết thúc: {e}")