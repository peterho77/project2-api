import asyncio
import random
import logging
from typing import Dict, Any, Optional
from aiohttp import ClientSession, ClientTimeout, ClientError
from fake_useragent import UserAgent

# ---------------------------------------------------------
# TÍCH HỢP HỆ THỐNG MỚI
# ---------------------------------------------------------
from config.settings import config
from src.json_handler import clean_description

# Khởi tạo Logger chuyên biệt theo yêu cầu của bạn
detail_logger = logging.getLogger("detail")

# UserAgent là tác vụ tốn tài nguyên khởi tạo, chỉ nên chạy 1 lần ở Module level
ua = UserAgent()

# Base Headers (Chỉ chứa các thông tin tĩnh, phần động sẽ được inject sau)
BASE_HEADERS = {
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


# ==========================================
# 1. CUSTOM EXCEPTIONS (Định nghĩa Lỗi API)
# ==========================================
class APIClientError(Exception):
    """Lỗi gốc cho module API Client."""
    pass


class RateLimitError(APIClientError):
    """Ném ra khi bị chặn HTTP 429."""
    pass


class DataExtractionError(APIClientError):
    """Ném ra khi dữ liệu JSON bị sai cấu trúc."""
    pass


# ==========================================
# 2. PURE FUNCTIONS (Data Parsing & Helpers)
# ==========================================
def build_dynamic_headers(base_headers: Dict[str, str]) -> Dict[str, str]:
    """Tạo headers mới an toàn với User-Agent ngẫu nhiên, không làm bẩn biến global."""
    headers = base_headers.copy()
    headers["User-Agent"] = ua.random
    return headers


async def extract_product_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Pure function bóc tách dữ liệu JSON thô thành schema chuẩn."""
    assert isinstance(raw_data, dict), "raw_data truyền vào phải là một Dictionary"

    try:
        images = raw_data.get("images", [])
        # List comprehension an toàn, kiểm tra kiểu dữ liệu của img
        images_url = [img.get("base_url") for img in images if isinstance(img, dict) and "base_url" in img]

        raw_desc = raw_data.get("description", "")
        # Chạy hàm parse HTML nặng ở một thread riêng để không block Event Loop
        clean_desc = await asyncio.to_thread(clean_description, raw_desc) if raw_desc else ""

        return {
            "id": raw_data.get("id"),
            "name": raw_data.get("name"),
            "url_key": raw_data.get("url_key"),
            "price": raw_data.get("price"),
            "description": clean_desc,
            "images_url": images_url
        }
    except Exception as e:
        raise DataExtractionError(f"Lỗi khi bóc tách schema JSON: {e}") from e


# ==========================================
# 3. CORE NETWORK WORKER
# ==========================================
async def fetch_product_info(
        session: ClientSession,
        product_id: int | str,
        # Tự động lấy config làm giá trị mặc định (Dependency Injection)
        api_url_template: str = config.get("API_URL", ""),
        max_retries: int = 3
) -> Dict[str, Any]:
    """
    Thực hiện HTTP Request với cơ chế Exponential Backoff.
    Tuân thủ nghiêm ngặt mô hình Try...Except...Else...Finally.
    """
    assert session is not None, "ClientSession không được để trống"
    assert api_url_template, "API_URL chưa được cấu hình trong config.yaml"

    url = api_url_template.format(product_id)
    timeout = ClientTimeout(total=15)

    for attempt in range(max_retries):
        raw_json_data = None

        try:
            # 1. Tránh burst request (delay 1.0 -> 3.0s)
            await asyncio.sleep(random.uniform(1.0, 3.0))

            # 2. Sinh header động
            current_headers = build_dynamic_headers(BASE_HEADERS)

            async with session.get(url, headers=current_headers, timeout=timeout) as response:
                status = response.status

                # --- PHÂN LOẠI LỖI CHỦ ĐỘNG (EARLY FAIL) ---
                if status == 429:
                    raise RateLimitError("HTTP 429: Too Many Requests")
                elif status == 404:
                    detail_logger.warning(f"Lỗi 404 ở ID {product_id} - Sản phẩm không tồn tại hoặc bị xóa.")
                    return {"error": True, "status_code": 404, "id": product_id}
                elif status >= 500:
                    raise ClientError(f"HTTP {status}: Lỗi từ hệ thống Server")

                # Chỉ khi status == 200 mới chạy xuống đây
                response.raise_for_status()
                raw_json_data = await response.json()

        # --- BẮT LỖI (EXCEPTION CATCHING) ---
        except RateLimitError:
            wait_time = (2 ** attempt) + random.uniform(1, 2)
            detail_logger.warning(f"Bị 429 ở ID {product_id}. Đang ngủ {wait_time:.1f}s (Lần thử {attempt + 1})...")
            await asyncio.sleep(wait_time)

        except asyncio.TimeoutError:
            detail_logger.warning(f"Timeout (15s) khi tải ID {product_id} (Lần thử {attempt + 1})")
            await asyncio.sleep(1.0)

        except ClientError as ce:
            detail_logger.warning(f"Lỗi mạng HTTP ở ID {product_id}: {ce} (Lần thử {attempt + 1})")
            await asyncio.sleep(2.0)

        except Exception as e:
            # Fatal Error: Các lỗi ngoại lệ như đứt kết nối vật lý, aiohttp crash...
            detail_logger.error(f"Lỗi Crash ngoại lệ ID {product_id}: {type(e).__name__} - {e}")
            return {"error": True, "status_code": "FATAL_ERROR", "id": product_id, "details": str(e)}

        # --- XỬ LÝ DATA (Chỉ chạy khi Network thành công hoàn toàn) ---
        else:
            try:
                # Bóc tách JSON an toàn trong khối else
                parsed_data = await extract_product_data(raw_json_data)
                return parsed_data
            except DataExtractionError as extract_err:
                detail_logger.error(f"Lỗi parse data ID {product_id}: {extract_err}")
                return {"error": True, "status_code": "PARSE_ERROR", "id": product_id}

        # --- TRACKING / AUDIT ---
        finally:
            detail_logger.debug(f"Kết thúc lượt thử {attempt + 1}/{max_retries} cho ID {product_id}.")

    # --- HẾT QUYỀN RETRY ---
    detail_logger.error(f"Bỏ qua ID {product_id} do đã cạn kiệt {max_retries} lần thử.")
    return {"error": True, "status_code": "EXHAUSTED", "id": product_id}