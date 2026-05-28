import asyncio
import random

from config.settings import config
from fake_useragent import UserAgent
from src.json_handler import clean_description

ua = UserAgent()

# config variable
api_url=config.get("API_URL")

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

                # Gửi lại request nếu response status khác 200 (429,404,....)
                else:
                    # NẾU BỊ 429: Tính toán thời gian nghỉ dài hơn sau mỗi lần thất bại (Exponential Backoff)
                    wait_time = (2 ** attempt) + random.uniform(0, 1)  # Ví dụ: 1s, 2s, 4s...
                    print(
                        f"[!] Bị 429 ở ID {product_id}. Đang ngủ {wait_time:.1f}s trước khi thử lại (Lần {attempt + 1})...")
                    await asyncio.sleep(wait_time)
                    continue  # Quay lại đầu vòng lặp để gửi lại request

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