import aiofiles
import json
import os
import re
import glob

# import from folder
from config.settings import config
from selectolax.parser import HTMLParser

# config variable
success_file=config.get("SUCCESS_FILE")
failed_file=config.get("FAILED_FILE")

def clean_description(html_content):
    """
    Chuẩn hoá nội dung description:
    - Loại bỏ các thẻ HTML.
    - Thay thế các khoảng trắng thừa, tab, newline liên tiếp thành định dạng gọn gàng.
    """
    if not html_content:
        return ""
    tree = HTMLParser(html_content)
    text = tree.text(strip=True)
    # Xoá khoảng trắng thừa
    return re.sub(r'\n+', '\n', text).strip()

async def file_writer(result_queue, chunk_size=1000):
    """Luồng chuyên trách gom đủ số lượng rồi ghi ra file JSON và lưu Checkpoint."""
    buffer = []
    chunk_index = 1

    # Tìm tất cả các file có tiền tố products_chunk_ và đuôi .json
    existing_files = glob.glob("products_chunk_*.json")

    if existing_files:
        max_idx = 0
        for f in existing_files:
            # Dùng Regex để tìm con số nằm ngay trước .json
            # Ví dụ: "products_chunk_6.json" -> lấy số 6
            match = re.search(r'_(\d+)\.json$', f)
            if match:
                idx = int(match.group(1))
                if idx > max_idx:
                    max_idx = idx

        # KẾT THÚC VÒNG LẶP, ĐÃ TÌM ĐƯỢC max_idx (Ví dụ: 144)
        last_file = f"products_chunk_{max_idx}.json"

        # BẮT ĐẦU KIỂM TRA DUNG LƯỢNG FILE CUỐI CÙNG
        if os.path.exists(last_file):
            file_size = os.path.getsize(last_file)

            # Nếu file hoàn toàn trống (0 byte) hoặc chỉ có mảng rỗng "[]" (thường < 5 bytes)
            if file_size < 5:
                print(f"[*] Phát hiện {last_file} trống ({file_size} bytes). Sẽ tái sử dụng và ghi đè!")
                chunk_index = max_idx  # Giữ nguyên số 144, KHÔNG cộng 1
            else:
                chunk_index = max_idx + 1  # File có dữ liệu thực sự, tạo số 145
        else:
            chunk_index = max_idx + 1

    success_ids = []  # Chứa ID thành công
    failed_ids = []  # Chứa ID bị lỗi (404, timeout, v.v.)

    async def save_data(js_buf, succ_ids, fail_ids, index):
        # Định nghĩa các đường dẫn thư mục chuẩn
        output_dir = os.path.join("data", "output")
        products_dir = os.path.join(output_dir, "products")

        # Tự động tạo thư mục nếu chưa tồn tại (tránh lỗi FileNotFoundError)
        os.makedirs(products_dir, exist_ok=True)

        # 1. Lưu file JSON nếu có dữ liệu
        if js_buf:
            filename = os.path.join(products_dir, f"products_chunk_{index}.json")
            async with aiofiles.open(filename, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(js_buf, ensure_ascii=False, indent=4))
            print(f"[+] Đã lưu {len(js_buf)} sản phẩm vào {filename}")

        # 2. Lưu ID thành công vào processed_ids.csv
        if succ_ids:
            saving_success_file = os.path.join(output_dir, success_file)
            async with aiofiles.open(saving_success_file, mode='a', encoding='utf-8') as f:
                for pid in succ_ids:
                    await f.write(f"{pid}\n")

        # 3. Lưu ID thất bại vào failed_ids.csv
        if fail_ids:
            saving_failed_file = os.path.join(output_dir, failed_file)
            async with aiofiles.open(saving_failed_file, mode='a', encoding='utf-8') as f:
                for pid in fail_ids:
                    await f.write(f"{pid}\n")

    # VÒNG LẶP CHÍNH
    while True:
        item = await result_queue.get()

        # Tín hiệu dừng (Poison Pill)
        if item is None:
            if buffer or success_ids or failed_ids:
                await save_data(buffer, success_ids, failed_ids, chunk_index)
            result_queue.task_done()
            break

        product_id, result = item

        # PHÂN LOẠI DỮ LIỆU VÀO CÁC BỘ ĐỆM
        if result is not None:
            buffer.append(result)
            success_ids.append(product_id)
        else:
            failed_ids.append(product_id)

        # Kiểm tra tổng số lượng ID đã xử lý (Cả thành công + thất bại)
        total_processed = len(success_ids) + len(failed_ids)

        if total_processed >= chunk_size:
            await save_data(buffer, success_ids, failed_ids, chunk_index)

            # Chỉ tăng số thứ tự file lên 1 NẾU thực sự có file JSON được tạo ra
            if buffer:
                chunk_index += 1

            # Xóa sạch các bộ đệm để chuẩn bị cho mảng tiếp theo
            buffer.clear()
            success_ids.clear()
            failed_ids.clear()

        result_queue.task_done()