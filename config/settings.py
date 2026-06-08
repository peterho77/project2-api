import yaml
import logging
import inspect
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict

# ==========================================
# 1. CỐ ĐỊNH PROJECT ROOT (BASE_DIR)
# ==========================================
CONFIG_DIR = Path(__file__).resolve().parent
PROJECT_BASE_DIR = CONFIG_DIR.parent  # Chỉnh số lượng .parent tùy vào cấu trúc thư mục của bạn

# Giả định config.yaml của bạn nằm ở ngay thư mục gốc (project/config.yaml)
CONFIG_YAML_PATH = CONFIG_DIR / 'config.yaml'


# ==========================================
# 2. QUẢN LÝ ĐƯỜNG DẪN ĐỘNG (Dynamic Path Management)
# ==========================================
@dataclass(frozen=True)
class DynamicPaths:
    """Lớp chứa đường dẫn, cur_dir sẽ thay đổi tùy file gọi."""
    cur_dir: Path
    base_dir: Path
    config_path: Path


def load_path() -> DynamicPaths:
    """
    Hàm tự động phát hiện file nào đang gọi nó để trả về cur_dir tương ứng.
    """
    # Dùng inspect để lấy "khung hình" (frame) của code gọi hàm này
    caller_frame = inspect.currentframe().f_back
    caller_file_path = caller_frame.f_globals.get('__file__')

    if caller_file_path:
        # Nếu được gọi từ một file .py bình thường (vd: a1.py)
        cur_dir = Path(caller_file_path).resolve().parent
    else:
        # Fallback an toàn nếu bạn đang chạy code trực tiếp trên terminal (Interactive Console)
        cur_dir = Path.cwd()

    return DynamicPaths(
        cur_dir=cur_dir,
        base_dir=PROJECT_BASE_DIR,  # Luôn luôn trỏ về gốc dự án
        config_path=CONFIG_YAML_PATH  # Luôn luôn trỏ về file config gốc
    )


# ==========================================
# 3. QUẢN LÝ CẤU HÌNH (Object Mapping)
# ==========================================
class AppConfig:
    def __init__(self, raw_dict: Dict[str, Any]):
        for key, value in raw_dict.items():
            setattr(self, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def load_config(path: Path) -> AppConfig:
    if not path.exists():
        logging.critical(f"[!] Lỗi nghiêm trọng: Không tìm thấy file {path}")
        raise SystemExit(1)

    try:
        with open(path, 'r', encoding='utf-8') as file:
            raw_data = yaml.safe_load(file) or {}
            return AppConfig(raw_data)
    except yaml.YAMLError as e:
        logging.critical(f"[!] Lỗi cú pháp trong file config.yaml: {e}")
        raise SystemExit(1)
    except Exception as e:
        logging.critical(f"[!] Lỗi ngoại lệ khi khởi tạo cấu hình: {e}")
        raise SystemExit(1)


# Khởi tạo config toàn cục một lần để các file khác import dùng chung
config = load_config(CONFIG_YAML_PATH)
PATHS = load_path()

# ĐỊNH NGHĨA SẴN CÁC THƯ MỤC CHUẨN CỦA DỰ ÁN (Single Source of Truth)
BASE_DIR = PATHS.base_dir
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"