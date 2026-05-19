import aiofiles
import asyncio
import random
import json
import os
import re
from bs4 import BeautifulSoup
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

    # Sử dụng lxml parser cho tốc độ xử lý nhanh hơn
    soup = BeautifulSoup(html_content, "lxml")

    # Lấy text và phân tách bằng newline
    text = soup.get_text(strip=True)

    # Xoá các khoảng trắng/dòng trống thừa mứa
    text = re.sub(r'\n+', '\n', text)
    return text.strip()


async def fetch_product_info(session, product_id, semaphore, max_retries=3):
    """Gửi request lấy thông tin của 1 sản phẩm."""
    async with semaphore:
        for attempt in range(max_retries):
            try:
                # Thêm một khoảng delay ngẫu nhiên nhỏ (0.2s - 0.7s) để giả lập người dùng,
                # tránh việc 7 request cùng lao vào server một lúc.
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


async def process_chunk(session, chunk_ids, chunk_index, semaphore):
    """Xử lý một batch 1000 ID và lưu ra file JSON."""
    tasks = [fetch_product_info(session, pid, semaphore) for pid in chunk_ids]
    results = await asyncio.gather(*tasks)

    # Lọc bỏ các kết quả None (những ID bị lỗi hoặc không tồn tại)
    valid_results = [res for res in results if res is not None]

    if not valid_results:
        print(f"[-] Chunk {chunk_index} không có dữ liệu hợp lệ.")
        return

    # Lưu ra file JSON
    filename = f"tiki_products_chunk_{chunk_index}.json"
    async with aiofiles.open(filename, mode='w', encoding='utf-8') as f:
        # json.dumps chuyển dict thành chuỗi trước, rồi mới ghi
        await f.write(json.dumps(valid_results, ensure_ascii=False, indent=4))

    print(f"[+] Đã lưu {len(valid_results)} sản phẩm vào {filename}")
