import aiofiles
import asyncio
import random
import json
import os
import re
import time
from selectolax.parser import HTMLParser
from dotenv import load_dotenv

load_dotenv()
api_url=os.getenv("API_URL")

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
            await asyncio.sleep(random.uniform(0.2, 0.7))

            async with session.get(api_url.format(product_id), headers=HEADERS, timeout=15) as response:
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

    # Tạo một ID phiên chạy (dựa trên thời gian) để tránh ghi đè file cũ khi chạy lại
    session_id = int(time.time())

    async def save_chunk(data_buffer, index):
        """Hàm phụ trợ để lưu file JSON và ghi log ID đã hoàn thành."""
        # 1. Ghi data ra file JSON
        filename = f"products_chunk_{session_id}_{index}.json"

        # Tách lấy phần dữ liệu thực tế (bỏ product_id dùng để track)
        data_to_save = [item[1] for item in data_buffer]

        async with aiofiles.open(filename, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data_to_save, ensure_ascii=False, indent=4))

        # 2. Ghi checkpoint các ID đã lưu thành công vào file processed_ids.txt
        async with aiofiles.open("processed_ids.txt", mode='a', encoding='utf-8') as f:
            for item in data_buffer:
                product_id = item[0]  # Lấy product_id từ tuple
                await f.write(f"{product_id}\n")

        print(f"[+] Đã lưu {len(data_buffer)} sản phẩm vào {filename} & cập nhật checkpoint.")

    while True:
        item = await result_queue.get()

        # Nhận được tín hiệu dừng (Poison Pill) từ hàm main
        if item is None:
            if buffer:
                # Lưu nốt những sản phẩm lẻ còn sót lại (nhỏ hơn chunk_size)
                await save_chunk(buffer, chunk_index)
            result_queue.task_done()
            break

        buffer.append(item)

        # Khi gom đủ số lượng items (VD: 1000)
        if len(buffer) >= chunk_size:
            await save_chunk(buffer, chunk_index)
            buffer.clear()  # Xóa bộ nhớ đệm để gom chunk tiếp theo
            chunk_index += 1

        result_queue.task_done()


async def worker(worker_id, job_queue, result_queue, session):
    """Worker lấy ID từ job_queue, tải dữ liệu, và đẩy vào result_queue."""
    while True:
        product_id = await job_queue.get()
        try:
            # Lưu ý: Hàm fetch_product_info của bạn giữ nguyên
            result = await fetch_product_info(session, product_id)
            if result:
                # THAY ĐỔI QUAN TRỌNG: Đẩy cả product_id và result vào queue dưới dạng Tuple
                # Để hàm file_writer biết chính xác ID nào để lưu checkpoint
                await result_queue.put((product_id, result))
        except Exception as e:
            print(f"[!] Lỗi ở worker {worker_id} ID {product_id}: {e}")
        finally:
            job_queue.task_done()

