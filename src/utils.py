import sys
import re
import time
import asyncio
import logging
import os
from pathlib import Path
from typing import List, Set, Dict, Optional

# ==========================================
# 1. CUSTOM EXCEPTIONS
# ==========================================
class UtilityBaseError(Exception):
    """Lỗi gốc cho toàn bộ module Utility."""
    pass


class FileSystemUtilityError(UtilityBaseError):
    """Lỗi khi tương tác tạo/xóa thư mục hệ thống."""
    pass


class LoggerSetupError(UtilityBaseError):
    """Lỗi khi không thể khởi tạo luồng ghi log."""
    pass


class FileTrackingError(UtilityBaseError):
    """Lỗi khi đọc/ghi các file CSV tracking (success, failed, dead)."""
    pass


# ==========================================
# 2. FILE SYSTEM UTILITIES (SRP)
# ==========================================
def ensure_directory_exists(dir_path: Path) -> None:
    """Đảm bảo thư mục tồn tại an toàn, ném lỗi rõ ràng nếu thất bại."""
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        raise FileSystemUtilityError(f"Không có quyền tạo thư mục tại {dir_path}: {e}") from e
    except OSError as e:
        raise FileSystemUtilityError(f"Lỗi hệ thống I/O khi tạo thư mục {dir_path}: {e}") from e


# ==========================================
# 3. LOGGER UTILITIES (Factory Pattern - OCP)
# ==========================================
def create_file_logger(
        logger_name: str,
        file_path: Path,
        fmt: str,
        datefmt: Optional[str] = None
) -> logging.Logger:
    """
    Factory Function: Thiết lập và trả về một đối tượng Logger.
    Hoàn toàn không chứa hardcode nghiệp vụ, dễ dàng tái sử dụng ở project khác.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Tránh tình trạng log bị nhân đôi (duplicate handlers) nếu gọi hàm nhiều lần
    if not logger.handlers:
        try:
            handler = logging.FileHandler(file_path, encoding='utf-8')
            formatter = logging.Formatter(fmt, datefmt=datefmt) if datefmt else logging.Formatter(fmt)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        except OSError as e:
            raise LoggerSetupError(f"Không thể gán FileHandler cho {file_path.name}: {e}") from e

    return logger


# ==========================================
# 4. DATA TRACKING & CLEANUP (Atomic Operations)
# ==========================================
def _read_ids_safe(filepath: Path) -> Set[str]:
    """Đọc file an toàn và dùng Regex gắp ID ra khỏi các chuỗi rác dính liền."""
    if not filepath.exists():
        return set()

    ids = set()
    # Biểu thức Regex: Chỉ tìm những cụm chứa các chữ số (0-9) đứng liền nhau
    number_pattern = re.compile(r'\d+')

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                val = line.strip()
                if not val:
                    continue

                # Quét dòng hiện tại, bóc tách toàn bộ các cụm số ra
                # Ví dụ: "nullnull215110865" -> trả về mảng ['215110865']
                extracted_numbers = number_pattern.findall(val)

                for num in extracted_numbers:
                    # Thêm điều kiện an toàn: ID Tiki thường có từ 5-6 số trở lên.
                    # Lọc bỏ các số linh tinh vô tình lọt vào.
                    if len(num) >= 5:
                        ids.add(num)

    except PermissionError as e:
        print(f"[!] Từ chối quyền truy cập khi đọc {filepath.name}: {e}")
    except OSError as e:
        print(f"[!] Lỗi I/O khi đọc {filepath.name}: {e}")

    return ids


def _write_ids_atomic(filepath: Path, data: Set[str]) -> None:
    """
    Kỹ thuật Atomic Write (Ghi nguyên tử): Ghi ra file tạm trước,
    thành công 100% mới ghi đè file chính. Tránh hỏng/mất dữ liệu nếu cúp điện giữa chừng.
    """
    temp_filepath = filepath.with_suffix('.tmp')
    try:
        with open(temp_filepath, 'w', encoding='utf-8') as f:
            f.writelines(f"{pid}\n" for pid in data)
        # Thay thế nguyên tử (Hoạt động an toàn trên cả Linux và Windows)
        os.replace(temp_filepath, filepath)
    except OSError as e:
        # Dọn rác file tạm nếu quá trình ghi gặp lỗi
        if temp_filepath.exists():
            temp_filepath.unlink(missing_ok=True)
        raise FileTrackingError(f"Lỗi khi ghi an toàn vào {filepath.name}: {e}") from e


def get_handled_ids(file_names: List[str], output_dir: Path) -> Set[str]:
    """Đọc và gom nhóm các ID đã xử lý từ nhiều file Tracking."""
    assert isinstance(output_dir, Path), "output_dir phải là một Path object"

    handled: Set[str] = set()
    for file_name in file_names:
        if not file_name:
            continue

        file_path = output_dir / file_name
        try:
            ids_from_file = _read_ids_safe(file_path)
            handled.update(ids_from_file)
        except FileTrackingError as e:
            # Lỗi đọc tracker rất nguy hiểm vì có thể gây cào lặp dữ liệu, cần báo cáo ngay
            logging.critical(f"Lỗi nghiêm trọng khi nạp lịch sử từ {file_name}: {e}")
            raise

    return handled


def cleanup_log_files(success_file: Path, failed_file: Path, dead_file: Path) -> None:
    """Thu dọn rác và lọc trùng lặp ID an toàn (Pure Logic & Set Operations)."""
    print("\n[*] Đang tổng vệ sinh và tối ưu hóa các file tracking (Checkpoint)...")

    try:
        # 1. Đọc an toàn vào RAM
        success_ids = _read_ids_safe(success_file)
        failed_ids = _read_ids_safe(failed_file)
        dead_ids = _read_ids_safe(dead_file)

        # 2. Xử lý Pure Logic - Phép trừ tập hợp
        true_dead_ids = dead_ids - success_ids
        true_failed_ids = failed_ids - success_ids - true_dead_ids

        # 3. Ghi an toàn (Atomic write) ra đĩa
        _write_ids_atomic(success_file, success_ids)
        _write_ids_atomic(failed_file, true_failed_ids)
        _write_ids_atomic(dead_file, true_dead_ids)

        print(
            f"[+] Dọn dẹp an toàn xong! Success: {len(success_ids)} | Cấp cứu: {len(true_failed_ids)} | Chết hẳn: {len(true_dead_ids)}.")

    except FileTrackingError as e:
        # Bắt lỗi I/O để chương trình không crash văng miểng lúc cuối cùng
        print(f"\n[!] CẢNH BÁO: Quá trình dọn dẹp file gặp sự cố: {e}")
        logging.error(f"Cleanup Failed: {e}")


# ==========================================
# 5. ASYNC TASKS & SYSTEM RECOVERY
# ==========================================
async def progress_reporter(
        progress_dict: Dict[str, int],
        total_ids: int,
        phase_name: str,
        progress_logger: logging.Logger
) -> None:
    """Tác vụ ngầm báo cáo tiến độ, bọc try-except toàn diện để tránh silent crash."""
    if total_ids <= 0:
        return

    last_log_time = time.time()
    try:
        while progress_dict.get("done", 0) < total_ids:
            done = progress_dict["done"]
            percent = (done / total_ids) * 100

            sys.stdout.write(f"\r[>] Tiến độ đang chạy: {done}/{total_ids} ID ({percent:.2f}%)    ")
            sys.stdout.flush()

            current_time = time.time()
            if current_time - last_log_time >= 180:
                progress_logger.info(f"[{phase_name}] Đang xử lý: {done}/{total_ids} ID ({percent:.2f}%)")
                last_log_time = current_time

            await asyncio.sleep(0.5)

    except asyncio.CancelledError:
        # Tác vụ bị hủy chủ động từ Orchestrator (main.py) khi job hoàn tất
        pass
    except Exception as e:
        progress_logger.error(f"Tác vụ báo cáo tiến độ bị lỗi ngoại lệ: {e}")


def check_and_recover_lock(
        lock_file_path: Path,
        summary_log_path: Path,
        summary_logger: logging.Logger
) -> None:
    """Đọc file Lock để khôi phục trạng thái nếu phiên chạy trước bị sập nguồn."""
    if not lock_file_path.exists():
        return

    content = ""
    try:
        with open(lock_file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if not content:
            return

        prev_start_time = float(content)

        if summary_log_path.exists():
            prev_end_time = summary_log_path.stat().st_mtime
        else:
            prev_end_time = prev_start_time

        elapsed_seconds = max(0, prev_end_time - prev_start_time)
        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

        summary_logger.info(f"Tình trạng: KẾT THÚC ĐỘT NGỘT DO SẬP NGUỒN (Khôi phục log)")
        summary_logger.info(f"Tổng thời gian chạy phiên trước: {time_str}")
        summary_logger.info("============================================\n")

    except ValueError:
        summary_logger.warning(f"File lock chứa dữ liệu không hợp lệ: '{content}'. Bỏ qua khôi phục.")
    except OSError as e:
        summary_logger.error(f"Không thể đọc file lock hoặc file log để khôi phục: {e}")
    except Exception as e:
        summary_logger.error(f"Quá trình khôi phục gặp lỗi không xác định: {e}")