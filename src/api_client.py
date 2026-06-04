import asyncio
import random
import logging

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

detail_logger = logging.getLogger("detail")

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

                # TRƯỜNG HỢP 2: LỖI 404 (Sản phẩm không tồn tại / Bị xóa)
                elif response.status == 404:
                    # Lỗi này có thử lại cũng vô ích -> Trả về lỗi luôn để tiết kiệm thời gian
                    msg = f"Lỗi 404 ở ID {product_id} - Không tồn tại."
                    # print(f"[-] {msg}")
                    detail_logger.warning(msg)
                    return {"error": True, "status_code": 404, "id": product_id}

                # TRƯỜNG HỢP 3: LỖI 429 (Bị chặn do quá tải / Rate Limit)
                elif response.status == 429:
                    # Tăng thời gian chờ lên theo cấp số nhân (Exponential Backoff)
                    wait_time = (2 ** attempt) + random.uniform(1, 2)
                    msg = f"Bị 429 ở ID {product_id}. Đang ngủ {wait_time:.1f}s (Lần {attempt + 1})..."
                    # print(f"[!] {msg}")
                    detail_logger.warning(msg)
                    await asyncio.sleep(wait_time)
                    continue  # Quay lại đầu vòng lặp để thử lại

                # TRƯỜNG HỢP 4: CÁC LỖI HTTP KHÁC (500, 502, 503...)
                else:
                    msg = f"Lỗi HTTP {response.status} ở ID {product_id} (Lần {attempt + 1})"
                    # print(f"[!] {msg}")
                    detail_logger.warning(msg)
                    await asyncio.sleep(2.0)
                    continue

        # TRƯỜNG HỢP 5: LỖI TIMEOUT (Chờ quá 15 giây không thấy phản hồi)
        except asyncio.TimeoutError:
            msg = f"Timeout khi tải ID {product_id} (Lần {attempt + 1})"
            #print(f"[!] {msg}")
            detail_logger.warning(msg)
            await asyncio.sleep(1.0)
            continue
        # TRƯỜNG HỢP 6: LỖI CRASH NGOẠI LỆ (Sai cấu trúc JSON, đứt mạng đột ngột...)
        except Exception as e:
            msg = f"Lỗi Crash ngoại lệ ID {product_id}: {e}"
            #print(f"[!] {msg}")
            detail_logger.error(msg)
            # Lỗi dạng này thường do dữ liệu dị dạng, trả về lỗi ngay không cần Retry
            return {"error": True, "status_code": "FATAL_ERROR", "id": product_id, "details": str(e)}

    # KẾT THÚC VÒNG LẶP: Nếu đã thử hết số lần (max_retries) mà vẫn bị continue xuống tới đây
    msg = f"Bỏ qua ID {product_id} do đã cạn kiệt {max_retries} lần thử."
    #print(f"[-] {msg}")
    detail_logger.error(msg)

    return {"error": True, "status_code": "EXHAUSTED", "id": product_id}