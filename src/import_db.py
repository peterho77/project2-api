import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from typing import List, Tuple, Dict, Any
from dotenv import load_dotenv

# ==========================================
# 1. KHỞI TẠO ĐƯỜNG DẪN & CẤU HÌNH CƠ BẢN
# ==========================================
from config.settings import BASE_DIR

# ==========================================
# 2. CUSTOM EXCEPTIONS
# ==========================================
class DatabaseImportError(Exception):
    """Lỗi gốc cho toàn bộ module Import Database."""
    pass


class DataValidationError(DatabaseImportError):
    """Lỗi xảy ra khi file JSON bị thiếu, hỏng hoặc sai cấu trúc nghiệp vụ."""
    pass


class DBConnectionError(DatabaseImportError):
    """Lỗi khi không thể kết nối tới PostgreSQL."""
    pass

# ==========================================
# 3. HELPER FUNCTIONS (Pure-like & SRP)
# ==========================================
def load_db_config(env_path: Path) -> Dict[str, str]:
    """Tải và đóng gói cấu hình Database một cách cô lập (No Side Effects)."""
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    return {
        "dbname": str(os.getenv("DB_NAME", "default_db")),
        "user": str(os.getenv("DB_USER", "")),
        "password": str(os.getenv("DB_PASSWORD", "")),
        "host": str(os.getenv("DB_HOST", "localhost")),
        "port": str(os.getenv("DB_PORT", "5432"))
    }


def get_json_files(products_dir: Path) -> List[Path]:
    """Quét thư mục lấy danh sách file JSON an toàn bằng pathlib."""
    assert products_dir.exists(), f"Thư mục không tồn tại: {products_dir}"

    files = list(products_dir.glob("products_chunk_*.json"))
    if not files:
        raise DataValidationError(f"Không tìm thấy file JSON nào tại {products_dir}")

    return files


def prepare_insert_data(file_path: Path) -> List[Tuple]:
    """
    Đọc và chuẩn hóa dữ liệu từ JSON sang Tuple để nạp vào DB.
    Hàm này không kết nối DB, hoàn toàn Testable bằng cách truyền file giả.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise DataValidationError(f"File {file_path.name} bị hỏng cấu trúc JSON: {e}") from e
    except OSError as e:
        raise DataValidationError(f"Lỗi hệ thống khi đọc file {file_path.name}: {e}") from e

    # Validation (Fail Fast)
    assert isinstance(data, list), f"Dữ liệu trong {file_path.name} phải là danh sách (List)."

    values = []
    for index, item in enumerate(data):
        if not item.get("id"):
            # Lỗi nghiệp vụ (Missing ID) -> Raise ValueError ngay lập tức
            raise ValueError(f"Dữ liệu dị dạng: Thiếu trường 'id' tại dòng {index} trong {file_path.name}")

        values.append((
            item.get("id"),
            item.get("name", ""),
            item.get("url_key", ""),
            item.get("price", 0),
            item.get("description", ""),
            item.get("images_url", [])
        ))

    return values


# ==========================================
# 4. CORE ORCHESTRATOR
# ==========================================
def process_and_insert_data(base_dir: Path, db_config: Dict[str, str]) -> None:
    """Điều phối vòng đời đọc file và nạp Database."""

    products_dir = base_dir / "data" / "output" / "products"
    insert_query = """
        INSERT INTO products (id, name, url_key, price, description, images_url)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            url_key = EXCLUDED.url_key,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            images_url = EXCLUDED.images_url;
    """

    conn = None
    total_inserted = 0

    print("[*] Đang khởi tạo tiến trình nạp dữ liệu...")

    # ==========================================
    # LUỒNG TRY...EXCEPT...ELSE...FINALLY NGHIÊM NGẶT
    # ==========================================
    try:
        # 1. Giai đoạn I/O ổ đĩa (Chạy trước khi mở Database)
        json_files = get_json_files(products_dir)
        all_values_to_insert = []

        for file_path in json_files:
            print(f"  -> Đang đọc và kiểm tra file: {file_path.name}")
            file_values = prepare_insert_data(file_path)
            all_values_to_insert.extend(file_values)

        if not all_values_to_insert:
            print("[-] Không có dữ liệu hợp lệ nào để nạp.")
            return

        # 2. Giai đoạn I/O Network (Mở Database)
        print(f"[*] Đang kết nối tới PostgreSQL để nạp {len(all_values_to_insert)} bản ghi...")

        try:
            conn = psycopg2.connect(**db_config)
        except psycopg2.OperationalError as e:
            raise DBConnectionError(f"Không thể kết nối đến Database: {e}") from e

        # Khối 'with conn' quản lý Transaction (Tự động Rollback nếu lỗi)
        with conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, all_values_to_insert)
                total_inserted = len(all_values_to_insert)

    # --- KHỐI BẮT LỖI TỪ CỤ THỂ ĐẾN TỔNG QUÁT ---
    except AssertionError as ae:
        print(f"\n[!] LỖI RÀNG BUỘC (Assert): {ae}")

    except ValueError as ve:
        print(f"\n[!] LỖI DỮ LIỆU (Nghiệp vụ): {ve}")

    except DataValidationError as dve:
        print(f"\n[!] LỖI FILE ĐẦU VÀO: {dve}")

    except DBConnectionError as dbce:
        print(f"\n[!] LỖI MẠNG: {dbce}")

    except psycopg2.Error as db_err:
        # Bắt mọi lỗi từ chính PostgreSQL (VD: sai kiểu dữ liệu cột, rớt mạng giữa chừng)
        print(f"\n[!] LỖI POSTGRESQL TRANSACTION: Transaction đã tự động được Rollback. Chi tiết: {db_err}")

    except Exception as e:
        # Lưới an toàn cuối cùng
        print(f"\n[!] LỖI KHÔNG XÁC ĐỊNH: Một sự cố hệ thống nghiêm trọng đã xảy ra: {e}")

    # --- KHỐI ELSE (Chỉ chạy khi 100% SUÔN SẺ) ---
    else:
        print(f"\n[+] TUYỆT VỜI! Đã xử lý và lưu thành công {total_inserted} sản phẩm vào Database.")

    # --- KHỐI FINALLY (Dọn dẹp Connection) ---
    finally:
        print("\n[*] Đang dọn dẹp tài nguyên...")
        if conn:
            conn.close()
            print("  - Đã đóng kết nối PostgreSQL an toàn.")
        print("[*] Kết thúc phiên làm việc.")


# ==========================================
# 5. ENTRY POINT (Dependency Injection)
# ==========================================
import os  # Dùng os ở đây chỉ để nạp fallback env nội bộ nếu cần thiết

if __name__ == "__main__":
    # Inject cấu hình từ .env
    ENV_PATH = BASE_DIR / "config" / ".env"
    db_configuration = load_db_config(ENV_PATH)

    # Thực thi
    process_and_insert_data(BASE_DIR, db_configuration)