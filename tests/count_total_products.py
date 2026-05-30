import os
import re
import json


def count_total_products_json():
    # 1. Tự động tìm thư mục gốc và trỏ tới data/output/products
    # Lùi 1 cấp từ thư mục chứa file code hiện hành (tests/) về thư mục gốc
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target_dir = os.path.join(BASE_DIR, 'data', 'output', 'products')

    # Kiểm tra xem thư mục có tồn tại không
    if not os.path.exists(target_dir):
        print(f"[LỖI] Không tìm thấy thư mục. Đang quét tại: '{target_dir}'")
        print("=> Vui lòng kiểm tra lại cấu trúc thư mục!")
        return

    # 2. Định nghĩa Regex để quét file đuôi .json
    pattern = re.compile(r'^products_chunk_.*\.json$')

    total_products = 0
    matched_files = []

    print(f"[*] Đang quét tìm các file JSON chunk tại: {target_dir} ...")

    # 3. Duyệt qua tất cả các file trong thư mục target_dir (thay vì thư mục '.')
    for filename in os.listdir(target_dir):
        if pattern.match(filename):
            # Cần lưu lại đường dẫn tuyệt đối của file để lát nữa mở file không bị lỗi
            full_path = os.path.join(target_dir, filename)
            matched_files.append(full_path)

    if not matched_files:
        print("[!] Không tìm thấy file nào khớp với định dạng 'products_chunk_*.json'.")
        return

    # Sắp xếp file theo thứ tự để log in ra dễ nhìn
    # Tự động trích xuất số để sắp xếp đúng logic (1, 2, 3... thay vì 1, 10, 2)
    def extract_number(filepath):
        # Lấy tên file từ đường dẫn đầy đủ trước khi dùng regex
        filename = os.path.basename(filepath)
        match = re.search(r'(\d+)\.json$', filename)
        return int(match.group(1)) if match else 0

    matched_files.sort(key=extract_number)

    print(f"[*] Tìm thấy {len(matched_files)} file. Đang đếm số lượng sản phẩm...\n")

    # 4. Mở từng file JSON và đếm số lượng object (sản phẩm) bên trong
    for file_path in matched_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                count = len(data)
                total_products += count

                # Chỉ in tên file cho gọn thay vì in cả đường dẫn dài
                filename_only = os.path.basename(file_path)
                print(f"  -> {filename_only}: {count} sản phẩm")
        except Exception as e:
            filename_only = os.path.basename(file_path)
            print(f"  [!] Lỗi khi đọc file {filename_only}: {e}")

    # 5. In kết quả tổng hợp
    print("\n==================================================")
    print(f"[*] TỔNG SỐ LƯỢNG SẢN PHẨM JSON: {total_products}")
    print("==================================================")


if __name__ == "__main__":
    count_total_products_json()