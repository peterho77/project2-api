import os
import glob
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Tìm và nạp các biến từ file .env vào hệ thống
load_dotenv()

# Lấy dữ liệu an toàn
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "default_db"), # Tham số thứ 2 là giá trị mặc định nếu không tìm thấy
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATTERN = os.path.join(BASE_DIR, "data", "output", "products_chunk_*.json")


def process_and_insert_data():
    conn = None
    total_inserted = 0

    try:
        print("[*] Đang khởi tạo tiến trình nạp dữ liệu...")

        # 1. TÌM FILE VÀ DÙNG ASSERT
        files = glob.glob(JSON_PATTERN)
        # Ràng buộc điều kiện: Bắt buộc phải có file, nếu không ném AssertionError
        assert len(files) > 0, f"Không tìm thấy file nào khớp với đường dẫn: {JSON_PATTERN}"

        # Mở kết nối Database
        print("[*] Đang kết nối tới PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)

        # ========================================================
        # SỬ DỤNG "WITH" CHO CONNECTION VÀ CURSOR
        # ========================================================
        # with conn: Tự động Rollback nếu có lỗi xảy ra bên trong khối này, tự Commit nếu trơn tru.
        with conn:
            # with conn.cursor(): Tự động đóng (close) cursor khi ra khỏi khối lệnh này.
            with conn.cursor() as cursor:

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

                # 2. XỬ LÝ DỮ LIỆU
                for file_path in files:
                    print(f"  -> Đang đọc file: {os.path.basename(file_path)}")

                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Ràng buộc điều kiện: File JSON phải là mảng
                    assert isinstance(data, list), f"Dữ liệu trong {file_path} bị sai định dạng (không phải List)."

                    values_to_insert = []

                    for index, item in enumerate(data):
                        # DÙNG RAISE: Chủ động ném ngoại lệ nếu dữ liệu dị dạng
                        if not item.get("id"):
                            raise ValueError(
                                f"Dữ liệu dị dạng: Không có trường 'id' tại dòng {index} trong {file_path}")

                        values_to_insert.append((
                            item.get("id"),
                            item.get("name", ""),
                            item.get("url_key", ""),
                            item.get("price", 0),
                            item.get("description", ""),
                            item.get("images_url", [])
                        ))

                    if values_to_insert:
                        execute_values(cursor, insert_query, values_to_insert)
                        total_inserted += len(values_to_insert)

    # ==========================================
    # CÁC BLOCK EXCEPT (Không cần conn.rollback() nữa vì 'with conn' đã lo)
    # ==========================================

    except AssertionError as ae:
        print(f"\n[!] LỖI RÀNG BUỘC (Assert): {ae}")

    except ValueError as ve:
        print(f"\n[!] LỖI DỮ LIỆU (Nghiệp vụ): {ve}")

    except FileNotFoundError:
        print(f"\n[!] LỖI HỆ THỐNG: Không tìm thấy thư mục hoặc file.")

    except json.JSONDecodeError as je:
        print(f"\n[!] LỖI ĐỌC FILE JSON (Cấu trúc hỏng): {je}")

    except psycopg2.Error as db_error:
        print(f"\n[!] LỖI DATABASE (PostgreSQL): {db_error}")

    except Exception as e:
        # BARE EXCEPTION: Tóm mọi lỗi bất ngờ (VD: Hết RAM, OS kill process...)
        print(f"\n[!] LỖI KHÔNG XÁC ĐỊNH: Một lỗi hệ thống nghiêm trọng đã xảy ra: {e}")

    # ==========================================
    # BLOCK ELSE: CHỈ CHẠY KHI KHÔNG CÓ BẤT KỲ LỖI NÀO
    # ==========================================
    else:
        # Nhờ 'with conn', đến được đây nghĩa là lệnh commit() đã âm thầm được gọi thành công!
        print(f"\n[+] TUYỆT VỜI! Đã xử lý và lưu thành công {total_inserted} sản phẩm vào Database.")

    # ==========================================
    # BLOCK FINALLY: LUÔN LUÔN DỌN DẸP
    # ==========================================
    finally:
        print("\n[*] Đang dọn dẹp tài nguyên...")

        # Cursor đã tự động được 'with' đóng, nên ta KHÔNG CẦN check và close cursor ở đây nữa.
        # Nhưng Connection thì phải tự tay đóng để trả tài nguyên cho Database.
        if conn:
            conn.close()
            print("  - Đã ngắt kết nối PostgreSQL an toàn.")

        print("[*] Kết thúc phiên làm việc.")


if __name__ == "__main__":
    process_and_insert_data()