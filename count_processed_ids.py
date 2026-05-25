import os

def count_ids_in_single_file(filepath):
    # Kiểm tra xem file có tồn tại không
    if not os.path.exists(filepath):
        print(f"[LỖI] Không tìm thấy file: '{filepath}'. Vui lòng kiểm tra lại tên hoặc đường dẫn!")
        return 0

    total_ids = 0
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            # Đếm các dòng còn lại
            total_ids = sum(1 for _ in f)

    except StopIteration:
        print("[THÔNG BÁO] File này trống hoặc chỉ có mỗi dòng tiêu đề.")
    except Exception as e:
        print(f"[LỖI] Đã xảy ra lỗi khi đọc file: {e}")

    return total_ids


# --- CHẠY CHƯƠNG TRÌNH ---
if __name__ == "__main__":
    # Yêu cầu người dùng nhập tên file hoặc đường dẫn
    file_input = input("Nhập tên file CSV (ví dụ: data.csv) hoặc kéo thả file vào đây: ").strip()

    # Mẹo nhỏ: Xóa dấu nháy kép/nháy đơn ở 2 đầu nếu bạn kéo thả file vào Terminal (Windows hay bị lỗi này)
    clean_filepath = file_input.strip('\'"')

    if clean_filepath:
        print(f"[*] Đang đọc file: {clean_filepath} ...")
        ids_count = count_ids_in_single_file(clean_filepath)
        print(f"=> Tổng số lượng ID trong file là: {ids_count}")
    else:
        print("[LỖI] Bạn chưa nhập tên file!")