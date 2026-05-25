import asyncio
import os
import pandas as pd
import glob
from unittest.mock import patch  # Bổ sung thư viện giả lập gõ phím input
from dotenv import load_dotenv

# Cấu hình file giả lập test
TEST_LOG = "test_processed_ids.txt"
TOTAL_IDS = 200  # Sẽ chia đều cho 2 file CSV

# Đẩy cấu hình vào môi trường
os.environ["CSV_COLUMN_NAME"] = "product_id"
os.environ["CHUNK_SIZE"] = "5"
os.environ["CONCURRENCY_LIMIT"] = "2"
os.environ["PROCESSED_FILE"] = TEST_LOG
os.environ["API_URL"] = "https://mock-api.com/{}"
os.environ["CSV_FILE_PATTERN"] = r'^test_products\d+\.csv$'

# Import code chính của bạn
from main import main, get_processed_ids
import tiki_scraper


def clean_test_environment():
    """Hàm chuyên trách dọn dẹp TẤT CẢ các file sinh ra trong lúc test"""
    # 1. Quét và xóa các file CSV dạng products*.csv (THÊM CHỮ s VÀO ĐÂY)
    csv_files = glob.glob("test_products*.csv")
    for f in csv_files:
        if os.path.exists(f):
            os.remove(f)

    # 2. Xóa file Log checkpoint
    if os.path.exists(TEST_LOG):
        os.remove(TEST_LOG)

    # 3. Quét và xóa toàn bộ các file JSON chunk sinh ra trong quá trình test
    chunk_files = glob.glob("products_chunk_*.json")
    for chunk in chunk_files:
        try:
            os.remove(chunk)
        except Exception:
            pass


def setup_test_environment():
    """Tạo 2 file CSV (product1.csv và product2.csv) chứa tổng cộng 200 ID"""
    clean_test_environment()

    # Tạo file product1.csv (100 ID đầu)
    df1 = pd.DataFrame({"product_id": [str(i) for i in range(100, 200)]})
    df1.to_csv("test_products1.csv", index=False)

    # Tạo file product2.csv (100 ID tiếp theo)
    df2 = pd.DataFrame({"product_id": [str(i) for i in range(200, 300)]})
    df2.to_csv("test_products2.csv", index=False)


# Giả lập worker tải nhanh để test
async def mock_fetch_product_info(session, product_id):
    await asyncio.sleep(0.1)
    return {"id": product_id, "name": f"Item {product_id}"}


tiki_scraper.fetch_product_info = mock_fetch_product_info


async def run_integration_test():
    setup_test_environment()
    print(f"[Test Setup] Đã tạo 2 file CSV (product1.csv và product2.csv) với tổng {TOTAL_IDS} ID.")

    print("\n--- BẮT ĐẦU ĐỢT 1: CHẠY VÀ BỊ NGẮT GIỮA CHỪNG ---")

    # Dùng patch để tự động "gõ" số 2 vào khi màn hình hiện input()
    with patch('builtins.input', return_value='2'):
        main_task = asyncio.create_task(main())

        # Cho chạy khoảng 2 giây để kịp lưu vài chunk rồi RÚT ĐIỆN
        await asyncio.sleep(2.0)
        main_task.cancel()

        try:
            await main_task
        except asyncio.CancelledError:
            print("[!] Đã ngắt tiến trình thành công (Giả lập mất điện).")

    # Đợi giải phóng RAM hoàn toàn
    await asyncio.sleep(2.0)

    # Đếm số ID lưu được ở Đợt 1
    processed_set_1 = get_processed_ids()
    print(f"\n[Kiểm tra] Số ID đã lưu sau đợt 1: {len(processed_set_1)}")

    if len(processed_set_1) == 0 or len(processed_set_1) >= TOTAL_IDS:
        print("[X] LỖI: Cấu hình sleep không phù hợp, đợt 1 chạy quá nhanh hoặc quá chậm.")
        return

    print("\n--- BẮT ĐẦU ĐỢT 2: KHỞI ĐỘNG LẠI ĐỂ CHẠY TIẾP ---")

    # Tự động gõ phím "2" cho Đợt chạy thứ 2
    with patch('builtins.input', return_value='2'):
        await main()

    print("\n--- ĐÁNH GIÁ KẾT QUẢ CUỐI CÙNG ---")
    # Thay vì dùng get_processed_ids() (dạng Set tự xóa trùng), ta đếm thẳng TỪNG DÒNG trong file log
    with open(TEST_LOG, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

    total_lines = len(lines)
    unique_ids = len(set(lines))

    print(f"Tổng số dòng được ghi vào file log: {total_lines}")
    print(f"Tổng số ID duy nhất (không trùng lặp): {unique_ids}")

    if total_lines == TOTAL_IDS and unique_ids == TOTAL_IDS:
        print("\n=======> [THÀNH CÔNG MỸ MÃN] <=======")
        print("1. Hệ thống đã quét ĐÚNG 2 FILE CSV.")
        print("2. Chạy tiếp sức đúng chỗ bị ngắt không sót một cái nào.")
        print("3. KHÔNG có ID nào bị cào lặp lại 2 lần!")
    else:
        print("\n=======> [THẤT BẠI] <=======")
        print("Dữ liệu lưu bị sai lệch số lượng hoặc bị trùng lặp.")

    clean_test_environment()
    print("[-] Hoàn tất dọn dẹp. Môi trường làm việc đã sạch sẽ!")


if __name__ == "__main__":
    asyncio.run(run_integration_test())