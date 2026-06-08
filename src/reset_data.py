"""
Module: Reset Environment
Author: Steven Ho
Description: Dọn dẹp dữ liệu và log để đưa hệ thống ETL về trạng thái ban đầu.
"""
import sys
from pathlib import Path
from typing import List

# Tích hợp hệ thống từ các module đã refactor
from config.settings import config, OUTPUT_DIR

# ==========================================
# 1. CUSTOM EXCEPTIONS
# ==========================================
class ResetEnvironmentError(Exception):
    """Lỗi gốc cho tiến trình dọn dẹp môi trường."""
    pass


class FileClearError(ResetEnvironmentError):
    """Lỗi khi không thể làm rỗng nội dung file Checkpoint/Log."""
    pass


class FileDeletionError(ResetEnvironmentError):
    """Lỗi khi không thể xóa vĩnh viễn file dữ liệu JSON."""
    pass


# ==========================================
# 2. FILE SYSTEM HELPER FUNCTIONS (SRP)
# ==========================================
def clear_file_content(file_path: Path) -> None:
    """Làm rỗng nội dung file (truncate về 0 byte) thay vì xóa hẳn."""
    if not file_path.exists():
        print(f"  [-] Bỏ qua (không tồn tại): {file_path.name}")
        return

    try:
        # Mở chế độ 'w' và không ghi gì cả -> Truncate file
        with open(file_path, 'w', encoding='utf-8') as f:
            pass
        print(f"  [v] Đã dọn sạch nội dung: {file_path.name}")
    except PermissionError as e:
        raise FileClearError(f"Từ chối quyền truy cập khi làm rỗng {file_path.name}: {e}") from e
    except OSError as e:
        raise FileClearError(f"Lỗi I/O khi làm rỗng {file_path.name}: {e}") from e


def delete_files_by_pattern(directory: Path, pattern: str) -> int:
    """
    Tìm và xóa vĩnh viễn các file khớp với pattern.
    Sử dụng generator của pathlib.Path.glob() để tiết kiệm RAM.
    """
    if not directory.exists():
        return 0

    deleted_count = 0
    for file_path in directory.glob(pattern):
        if file_path.is_file():
            try:
                file_path.unlink()  # Tương đương os.remove()
                deleted_count += 1
                print(f"  [x] Đã xóa vĩnh viễn: {file_path.name}")
            except PermissionError as e:
                raise FileDeletionError(f"Không có quyền xóa {file_path.name}: {e}") from e
            except OSError as e:
                raise FileDeletionError(f"Lỗi I/O hệ thống khi xóa {file_path.name}: {e}") from e

    return deleted_count


# ==========================================
# 3. CORE RESET ORCHESTRATOR
# ==========================================
def reset_environment() -> None:
    """Hàm điều phối tiến trình dọn dẹp môi trường làm việc."""
    print("=======================================")
    print("    BẮT ĐẦU DỌN DẸP MÔI TRƯỜNG TIKI")
    print("=======================================\n")

    # 1. Khởi tạo đường dẫn từ settings
    output_dir = OUTPUT_DIR
    products_dir = OUTPUT_DIR / "products"

    # Lấy tên file tracking linh hoạt từ file config.yaml
    files_to_clear: List[Path] = [
        output_dir / config.get("SUCCESS_FILE", "success_ids.csv"),
        output_dir / config.get("FAILED_FILE", "failed_ids.csv"),
        output_dir / config.get("DEAD_FILE", "dead_ids.csv"),
        output_dir / "detail.log",
        output_dir / "summary.log",
        output_dir / "progress.log",
        output_dir / "running.lock"  # Xóa luôn file lock nếu có
    ]

    try:
        # 2. GIAI ĐOẠN 1: Làm rỗng các file Tracking và Log
        print("[*] Đang làm rỗng các file Checkpoint & Log...")
        for file_path in files_to_clear:
            clear_file_content(file_path)

        # 3. GIAI ĐOẠN 2: Xóa các file dữ liệu JSON
        print("\n[*] Đang xóa vĩnh viễn các file dữ liệu JSON...")
        total_deleted = 0

        # Quét thư mục gốc (output_dir) và thư mục con (products_dir)
        total_deleted += delete_files_by_pattern(output_dir, "*.json")
        total_deleted += delete_files_by_pattern(products_dir, "*.json")

        # 4. REPORT
        print("\n---------------------------------------")
        print(f"[+] HOÀN TẤT! Đã làm rỗng các file Tracking/Log và xóa {total_deleted} file JSON.")
        print("Hệ thống đã trở về trạng thái như mới, sẵn sàng cho lần chạy tiếp theo!\n")

    # Bắt các Custom Exceptions để in lỗi tường minh
    except ResetEnvironmentError as re_err:
        print(f"\n[!] LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH DỌN DẸP: {re_err}")
        print("Hãy kiểm tra xem có phần mềm nào đang mở các file này không, rồi chạy lại script.")
        sys.exit(1)

    except Exception as e:
        print(f"\n[!] LỖI NGOẠI LỆ KHÔNG LƯỜNG TRƯỚC: {e}")
        sys.exit(1)


if __name__ == "__main__":
    reset_environment()