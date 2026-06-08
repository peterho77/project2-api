import re
import json
import logging
import asyncio
import aiofiles
from pathlib import Path
from typing import List, Any, Dict, Optional
from selectolax.parser import HTMLParser

# ---------------------------------------------------------
# TÍCH HỢP SETTINGS MỚI
# ---------------------------------------------------------

# Lấy tự động hệ thống đường dẫn (Dynamic Paths)
from config.settings import config, BASE_DIR

# ==========================================
# 1. CUSTOM EXCEPTIONS
# ==========================================
class FileSystemError(Exception):
    """Lỗi liên quan đến đọc/ghi file hoặc cấp quyền hệ thống."""
    pass


class DataParsingError(Exception):
    """Lỗi khi xử lý hoặc làm sạch dữ liệu văn bản/HTML."""
    pass


# ==========================================
# 2. PURE FUNCTIONS (Data Processing)
# ==========================================
def clean_description(html_content: Optional[str]) -> str:
    """Chuẩn hoá nội dung description (No Side Effects)."""
    if not html_content:
        return ""

    assert isinstance(html_content, str), "Nội dung đầu vào phải là chuỗi (string)"

    try:
        tree = HTMLParser(html_content)
        text = tree.text(strip=True)
    except Exception as e:
        raise DataParsingError(f"Lỗi parse HTML bằng selectolax: {e}") from e
    else:
        return re.sub(r'\n+', '\n', text).strip()


# ==========================================
# 3. PATHLIB HELPER FUNCTIONS (SRP)
# ==========================================
def get_next_chunk_index(products_dir: Path) -> int:
    """Tách riêng logic tìm kiếm chunk_index tiếp theo (Sử dụng Pathlib)."""
    assert products_dir.exists(), "Thư mục products không tồn tại"

    # Sử dụng generator .glob() của pathlib thay cho thư viện glob cũ
    existing_files = list(products_dir.glob("products_chunk_*.json"))

    if not existing_files:
        return 1

    max_idx = 0
    for f in existing_files:
        # f.name lấy đúng tên file (vd: products_chunk_1.json)
        match = re.search(r'_(\d+)\.json$', f.name)
        if match:
            max_idx = max(max_idx, int(match.group(1)))

    if max_idx == 0:
        return 1

    # Kiểm tra file cuối cùng bằng pathlib (.stat().st_size)
    last_file = products_dir / f"products_chunk_{max_idx}.json"
    try:
        if last_file.exists():
            file_size = last_file.stat().st_size
            if file_size < 5:
                logging.info(f"Phát hiện {last_file.name} rỗng ({file_size} bytes). Tái sử dụng index {max_idx}.")
                return max_idx
    except OSError as e:
        logging.warning(f"Không thể kiểm tra dung lượng file {last_file.name}: {e}")

    return max_idx + 1


async def save_checkpoint(
        buffer: List[Dict],
        success_ids: List[Any],
        failed_ids: List[Any],
        chunk_index: int,
        products_dir: Path,
        output_dir: Path,
        success_filename: str,
        fail_filename: str
) -> None:
    """Logic ghi file bất đồng bộ sử dụng chuẩn Pathlib."""
    assert products_dir.exists(), "Thư mục products_dir không tồn tại trước khi ghi"

    try:
        # 1. Ghi JSON Buffer
        if buffer:
            json_file = products_dir / f"products_chunk_{chunk_index}.json"
            async with aiofiles.open(json_file, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(buffer, ensure_ascii=False, indent=4))
            logging.info(f"[+] Đã lưu {len(buffer)} sản phẩm vào {json_file.name}")

        # 2. Ghi IDs thành công
        if success_ids:
            succ_path = output_dir / success_filename
            async with aiofiles.open(succ_path, mode='a', encoding='utf-8') as f:
                data_to_write = "".join(f"{pid}\n" for pid in success_ids)
                await f.write(data_to_write)

        # 3. Ghi IDs thất bại
        if failed_ids:
            fail_path = output_dir / fail_filename
            async with aiofiles.open(fail_path, mode='a', encoding='utf-8') as f:
                data_to_write = "".join(f"{pid}\n" for pid in failed_ids)
                await f.write(data_to_write)

    except PermissionError as e:
        raise FileSystemError(f"Không có quyền ghi file tại {output_dir}: {e}") from e
    except TypeError as e:
        raise DataParsingError(f"Dữ liệu không thể serialize thành JSON: {e}") from e
    except Exception as e:
        raise FileSystemError(f"Lỗi ngoại lệ I/O không lường trước: {e}") from e


# ==========================================
# 4. CORE CONSUMER WORKER (Dependency Injection)
# ==========================================
async def file_writer(
        result_queue: asyncio.Queue,
        # Tự động nạp giá trị từ settings.py làm giá trị mặc định
        base_dir: Path = BASE_DIR,
        success_file: str = config.get("SUCCESS_FILE", "success_ids.csv"),
        target_fail_file: str = config.get("FAILED_FILE", "failed_ids.csv"),
        chunk_size: int = 1000,
        max_fail_ids_limit: int = 200
) -> None:
    """Luồng I/O Worker kết nối mượt mà với Object Config và Pathlib."""

    assert chunk_size > 0, "chunk_size phải lớn hơn 0"
    assert max_fail_ids_limit > 0, "max_fail_ids_limit phải lớn hơn 0"

    output_dir = base_dir / "data" / "output"
    products_dir = output_dir / "products"

    try:
        # mkdir(parents=True) thay thế hoàn hảo cho os.makedirs
        products_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Không thể tạo cấu trúc thư mục đầu ra: {e}")
        return

    chunk_index = get_next_chunk_index(products_dir)
    buffer: List[Dict] = []
    success_ids: List[Any] = []
    failed_ids: List[Any] = []

    while True:
        try:
            item = await result_queue.get()

            # --- POISON PILL ---
            if item is None:
                if buffer or success_ids or failed_ids:
                    await save_checkpoint(
                        buffer, success_ids, failed_ids, chunk_index,
                        products_dir, output_dir, success_file, target_fail_file
                    )
                result_queue.task_done()
                logging.info("Tín hiệu kết thúc nhận được. File Writer ngừng hoạt động.")
                break

            product_id, result = item

            if result is not None:
                buffer.append(result)
                success_ids.append(product_id)
            else:
                failed_ids.append(product_id)

            # --- ĐIỀU KIỆN LƯU ---
            if len(buffer) >= chunk_size:
                await save_checkpoint(
                    buffer, success_ids, failed_ids, chunk_index,
                    products_dir, output_dir, success_file, target_fail_file
                )
                chunk_index += 1
                buffer.clear()
                success_ids.clear()
                failed_ids.clear()

            elif len(failed_ids) >= max_fail_ids_limit:
                await save_checkpoint(
                    [], [], failed_ids, chunk_index,
                    products_dir, output_dir, success_file, target_fail_file
                )
                failed_ids.clear()

        except FileSystemError as fs_err:
            logging.critical(f"LỖI HỆ THỐNG FILE NGHIÊM TRỌNG: {fs_err}")
            break

        except Exception as e:
            logging.error(f"Lỗi ngoại lệ trong quá trình xử lý item từ queue: {e}")

        finally:
            if item is not None:
                result_queue.task_done()