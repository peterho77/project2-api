import os

def count_ids_in_single_file(filepath):
    """Đếm tổng số ID và số ID duy nhất trong file."""
    # Kiểm tra xem file có tồn tại không
    if not os.path.exists(filepath):
        print(f"[LỖI] Không tìm thấy file: '{filepath}'. Vui lòng kiểm tra lại tên hoặc đường dẫn!")
        return 0, 0  # Trả về 2 giá trị 0

    total_ids = 0
    unique_ids_set = set()  # Khởi tạo một Set rỗng để lưu ID duy nhất

    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Cắt bỏ khoảng trắng và dấu xuống dòng ở 2 đầu
                clean_line = line.strip()

                # Bỏ qua các dòng trống (nếu có) để tránh đếm sai
                if clean_line:
                    total_ids += 1
                    unique_ids_set.add(clean_line)

    except Exception as e:
        print(f"[LỖI] Đã xảy ra lỗi khi đọc file: {e}")

    unique_ids = len(unique_ids_set)
    return total_ids, unique_ids


# --- CHẠY CHƯƠNG TRÌNH ---
if __name__ == "__main__":
    # Yêu cầu người dùng nhập tên file hoặc đường dẫn
    file_input = input("Nhập tên file CSV (ví dụ: data.csv) hoặc kéo thả file vào đây: ").strip()

    # Mẹo nhỏ: Xóa dấu nháy kép/nháy đơn ở 2 đầu nếu kéo thả file vào Terminal
    clean_filepath = file_input.strip('\'"')

    if clean_filepath:
        print(f"[*] Đang đọc file: {clean_filepath} ...")

        # Nhận 2 giá trị trả về từ hàm
        total, unique = count_ids_in_single_file(clean_filepath)

        print("-" * 40)
        print(f"=> Tổng số lượng ID đã quét : {total}")
        print(f"=> Tổng số ID DUY NHẤT      : {unique}")

        # Tính toán thêm số lượng bị trùng cho trực quan
        duplicates = total - unique
        if duplicates > 0:
            print(f"[!] Cảnh báo: Có {duplicates} ID bị trùng lặp trong file này.")
        else:
            print("[+] Tuyệt vời: File rất sạch, không có ID nào bị trùng!")
        print("-" * 40)

    else:
        print("[LỖI] Bạn chưa nhập tên file!")