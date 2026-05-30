import os
import glob


def reset_environment():
    print("=======================================")
    print("    BẮT ĐẦU DỌN DẸP MÔI TRƯỜNG TIKI")
    print("=======================================\n")

    # 1. Định nghĩa đường dẫn
    # Lấy thư mục gốc nơi chứa file reset_data.py này
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))  # Thư mục chứa file này (src/)
    BASE_DIR = os.path.dirname(CURRENT_DIR)
    output_dir = os.path.join(BASE_DIR, "data", "output")
    products_dir = os.path.join(output_dir, "products")

    # 2. Xóa sạch dữ liệu (Clear nội dung) của các file tracking
    files_to_clear = [
        os.path.join(output_dir, "success_ids.csv"),
        os.path.join(output_dir, "failed_ids.csv"),
        os.path.join(output_dir, "report.log")
    ]

    print("[*] Đang làm rỗng các file Checkpoint & Log...")
    for file_path in files_to_clear:
        if os.path.exists(file_path):
            try:
                # Mở chế độ 'w' (write) và không ghi gì cả -> File sẽ bị xóa sạch nội dung về 0 byte
                with open(file_path, 'w', encoding='utf-8') as f:
                    pass
                print(f"  [v] Đã dọn sạch: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"  [!] Lỗi khi dọn {os.path.basename(file_path)}: {e}")
        else:
            print(f"  [-] Bỏ qua (không tồn tại): {os.path.basename(file_path)}")

    # 3. Tìm và xóa vĩnh viễn các file JSON (Xóa hẳn file)
    print("\n[*] Đang xóa các file dữ liệu JSON...")

    # Tìm JSON ở cả 2 nơi: data/output/ và data/output/products/
    json_patterns = [
        os.path.join(output_dir, "*.json"),
        os.path.join(products_dir, "*.json")
    ]

    deleted_count = 0
    for pattern in json_patterns:
        # glob.glob sẽ lấy ra danh sách tất cả các file khớp với đuôi .json
        for json_file in glob.glob(pattern):
            try:
                os.remove(json_file)
                deleted_count += 1
                # In tên file vừa xóa để bạn dễ theo dõi
                print(f"  [x] Đã xóa file: {os.path.basename(json_file)}")
            except Exception as e:
                print(f"  [!] Không thể xóa {os.path.basename(json_file)}: {e}")

    print("---------------------------------------")
    print(f"[+] HOÀN TẤT! Đã dọn sạch 3 file log và xóa {deleted_count} file JSON.")
    print("Hệ thống đã trở về trạng thái như mới, sẵn sàng cho lần chạy tiếp theo!\n")


if __name__ == "__main__":
    reset_environment()