import os
import re
import json


def count_total_products_json():
    # 1. Định nghĩa Regex để quét file đuôi .json
    pattern = re.compile(r'^products_chunk_.*\.json$')

    total_products = 0
    matched_files = []

    print("Đang quét thư mục tìm các file JSON chunk...")

    # 2. Duyệt qua tất cả các file trong thư mục hiện tại
    for filename in os.listdir('.'):
        if pattern.match(filename):
            matched_files.append(filename)

    if not matched_files:
        print("[!] Không tìm thấy file nào khớp với định dạng 'products_chunk_*.json'.")
        return

    # Sắp xếp file theo thứ tự để log in ra dễ nhìn
    # Tự động trích xuất số để sắp xếp đúng logic (1, 2, 3... thay vì 1, 10, 2)
    def extract_number(filename):
        match = re.search(r'(\d+)\.json$', filename)
        return int(match.group(1)) if match else 0

    matched_files.sort(key=extract_number)

    print(f"[*] Tìm thấy {len(matched_files)} file. Đang đếm số lượng sản phẩm...\n")

    # 3. Mở từng file JSON và đếm số lượng object (sản phẩm) bên trong
    for file in matched_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                count = len(data)
                total_products += count
                print(f"  -> {file}: {count} sản phẩm")
        except Exception as e:
            print(f"  [!] Lỗi khi đọc file {file}: {e}")

    # 4. In kết quả tổng hợp
    print("\n==================================================")
    print(f"[*] TỔNG SỐ LƯỢNG SẢN PHẨM JSON: {total_products}")
    print("==================================================")


if __name__ == "__main__":
    count_total_products_json()