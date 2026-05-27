import yaml
import os

# 1. Tìm đường dẫn tuyệt đối đến thư mục gốc của dự án
# __file__ là đường dẫn của file settings.py hiện tại
# Dùng dirname 2 lần để lùi từ /config/settings.py ra thư mục gốc
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 2. Tạo đường dẫn chính xác đến file config.yaml
CONFIG_PATH = os.path.join(BASE_DIR, 'config.yaml')

def load_config(path):
    """Hàm đọc file YAML và trả về Dictionary."""
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file) or {}
    except FileNotFoundError:
        print(f"[!] Lỗi nghiêm trọng: Không tìm thấy file {path}")
        exit(1)
    except yaml.YAMLError as e:
        print(f"[!] Lỗi cú pháp trong file config.yaml: {e}")
        exit(1)

# 3. Chạy hàm lấy dữ liệu VÀ LƯU VÀO BIẾN `CONFIG`
# Các file khác sẽ chỉ cần import biến `CONFIG` này để dùng
config = load_config(CONFIG_PATH)