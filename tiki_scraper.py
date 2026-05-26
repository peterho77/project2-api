import aiofiles
import asyncio
import random
import json
import os
import re
import glob
from selectolax.parser import HTMLParser
from dotenv import load_dotenv
from fake_useragent import UserAgent

ua = UserAgent()
load_dotenv()
api_url=os.getenv("API_URL")
processed_file=os.getenv("PROCESSED_FILE")
failed_file=os.getenv("FAILED_FILE")

# Sử dụng User-Agent thực để bypass các bộ lọc bot cơ bản
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://tiki.vn/",
    "Origin": "https://tiki.vn",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

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


async def fetch_product_info(session, product_id, max_retries=3):
    """Gửi request lấy thông tin của 1 sản phẩm."""
    for attempt in range(max_retries):
        try:
            # Thêm một khoảng delay ngẫu nhiên nhỏ (0.2s - 0.7s) để giả lập người dùng,
            # tránh việc 50 request cùng lao vào server một lúc.
            await asyncio.sleep(random.uniform(1.0, 3.0))

            # Tự động tạo HEADERS mới có User-Agent ngẫu nhiên cho mỗi request
            current_headers = HEADERS.copy()
            current_headers["User-Agent"] = ua.random

            async with session.get(api_url.format(product_id), headers=current_headers, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    images = data.get("images", [])
                    images_url = [img.get("base_url") for img in images if "base_url" in img]
                    clean_desc = await asyncio.to_thread(clean_description, data.get("description"))

                    return {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_desc,
                        "images_url": images_url
                    }

                elif response.status == 429:
                    # NẾU BỊ 429: Tính toán thời gian nghỉ dài hơn sau mỗi lần thất bại (Exponential Backoff)
                    wait_time = (2 ** attempt) + random.uniform(0, 1)  # Ví dụ: 1s, 2s, 4s...
                    print(
                        f"[!] Bị 429 ở ID {product_id}. Đang ngủ {wait_time:.1f}s trước khi thử lại (Lần {attempt + 1})...")
                    await asyncio.sleep(wait_time)
                    continue  # Quay lại đầu vòng lặp để gửi lại request

                else:
                    print(f"[!] Lỗi HTTP {response.status} cho ID {product_id}")
                    return None

        except asyncio.TimeoutError:
            print(f"[!] Timeout khi tải ID {product_id} (Lần {attempt + 1})")
            await asyncio.sleep(1)  # Nghỉ 1s rồi thử lại
            continue
        except Exception as e:
            print(f"[!] Lỗi ngoại lệ ID {product_id}: {e}")
            return None

    # Nếu đã thử hết max_retries mà vẫn thất bại
    print(f"[-] Bỏ qua ID {product_id} sau {max_retries} lần thử thất bại.")
    return None


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
        """Hàm phụ trợ lưu JSON và 2 file Checkpoint riêng biệt."""

        # 1. Lưu file JSON nếu có dữ liệu
        if js_buf:
            filename = f"products_chunk_{index}.json"
            async with aiofiles.open(filename, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(js_buf, ensure_ascii=False, indent=4))
            print(f"[+] Đã lưu {len(js_buf)} sản phẩm vào {filename}")

        # 2. Lưu ID thành công vào processed_ids.csv
        if succ_ids:
            async with aiofiles.open(processed_file, mode='a', encoding='utf-8') as f:
                for pid in succ_ids:
                    await f.write(f"{pid}\n")

        # 3. Lưu ID thất bại vào failed_ids.csv
        if fail_ids:
            async with aiofiles.open(failed_file, mode='a', encoding='utf-8') as f:
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


async def worker(worker_id, job_queue, result_queue, session):
    """Worker lấy ID từ job_queue, tải dữ liệu, và đẩy vào result_queue."""
    while True:
        product_id = await job_queue.get()
        try:
            # Lưu ý: Hàm fetch_product_info của bạn giữ nguyên
            result = await fetch_product_info(session, product_id)
            await result_queue.put((product_id, result))
        except Exception as e:
            print(f"[!] Lỗi ở worker {worker_id} ID {product_id}: {e}")
        finally:
            job_queue.task_done()

