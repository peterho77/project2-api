import asyncio
import os
import pandas as pd
import glob
from unittest.mock import patch

# 1. Cấu hình file giả lập test trỏ về thư mục data/output/
TEST_PROCESSED_FILE = os.path.join("data", "output", "test_processed_ids.csv")
TEST_FAILED_FILE = os.path.join("data", "output", "test_failed_ids.csv")
TOTAL_IDS = 200

# 2. Import from folder src
import src.utils as main_module
from src.utils import main, get_handled_ids

def clean_test_environment():
    """Hàm dọn dẹp TẤT CẢ các file sinh ra trong lúc test ở thư mục data/"""
    # 1. Quét và xóa các file CSV dạng test_products*.csv ở data/input/
    input_csvs = glob.glob(os.path.join("data", "input", "test_products*.csv"))
    for f in input_csvs:
        if os.path.exists(f):
            os.remove(f)

    # 2. Xóa file Log checkpoint ở data/output/
    if os.path.exists(TEST_PROCESSED_FILE): os.remove(TEST_PROCESSED_FILE)
    if os.path.exists(TEST_FAILED_FILE): os.remove(TEST_FAILED_FILE)

    # 3. Quét và xóa toàn bộ file JSON test ở data/output/products/
    chunk_files = glob.glob(os.path.join("data", "output", "products", "products_chunk_*.json"))
    for chunk in chunk_files:
        try:
            os.remove(chunk)
        except Exception:
            pass


def setup_test_environment():
    """Tạo 2 file CSV chứa 200 ID vào thư mục data/input/"""
    # Đảm bảo các thư mục đã tồn tại trước khi tạo file
    os.makedirs(os.path.join("data", "input"), exist_ok=True)
    os.makedirs(os.path.join("data", "output", "products"), exist_ok=True)

    clean_test_environment()

    # Tạo file test_products1.csv
    df1 = pd.DataFrame({"product_id": [str(i) for i in range(100, 200)]})
    df1.to_csv(os.path.join("data", "input", "test_products1.csv"), index=False)

    # Tạo file test_products2.csv
    df2 = pd.DataFrame({"product_id": [str(i) for i in range(200, 300)]})
    df2.to_csv(os.path.join("data", "input", "test_products2.csv"), index=False)

    # --- Ép kiểu các biến trong file main.py để phục vụ test ---
    main_module.chunk_size = 5
    main_module.concurrency_limit = 2
    main_module.processed_file = TEST_PROCESSED_FILE
    main_module.failed_file = TEST_FAILED_FILE
    os.environ["CSV_FILE_PATTERN"] = r'^test_products\d+\.csv$'


# Giả lập worker tải nhanh để test mà không cần gọi internet
async def mock_fetch_product_info(session, product_id):
    await asyncio.sleep(0.1)
    return {"id": product_id, "name": f"Item {product_id}"}


async def run_integration_test():
    setup_test_environment()
    print(f"[Test Setup] Đã tạo 2 file CSV test với tổng {TOTAL_IDS} ID.")

    print("\n--- BẮT ĐẦU ĐỢT 1: CHẠY VÀ BỊ NGẮT GIỮA CHỪNG ---")

    # Dùng patch để giả lập nhập phím 2, đồng thời tráo hàm fetch_product_info bằng mock
    with patch('builtins.input', return_value='2'), \
            patch('src.main.fetch_product_info', side_effect=mock_fetch_product_info):

        main_task = asyncio.create_task(main())

        # Cho chạy khoảng 1.5 - 2 giây để kịp lưu vài chunk rồi RÚT ĐIỆN
        await asyncio.sleep(1.5)
        main_task.cancel()

        try:
            await main_task
        except asyncio.CancelledError:
            print("[!] Đã ngắt tiến trình thành công (Giả lập mất điện).")

    # Đợi giải phóng RAM hoàn toàn
    await asyncio.sleep(2.0)

    # Đếm số ID lưu được ở đợt 1 (gọi đúng hàm get_handled_ids)
    processed_set_1 = get_handled_ids([TEST_PROCESSED_FILE])
    print(f"\n[Kiểm tra] Số ID đã lưu sau đợt 1: {len(processed_set_1)}")

    if len(processed_set_1) == 0 or len(processed_set_1) >= TOTAL_IDS:
        print("[X] LỖI: Cấu hình sleep không phù hợp, đợt 1 chạy quá nhanh hoặc quá chậm.")
        return

    print("\n--- BẮT ĐẦU ĐỢT 2: KHỞI ĐỘNG LẠI ĐỂ CHẠY TIẾP ---")

    with patch('builtins.input', return_value='2'), \
            patch('src.main.fetch_product_info', side_effect=mock_fetch_product_info):
        await main()

    print("\n--- ĐÁNH GIÁ KẾT QUẢ CUỐI CÙNG ---")

    with open(TEST_PROCESSED_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

    total_lines = len(lines)
    unique_ids = len(set(lines))

    print(f"Tổng số dòng được ghi vào file log: {total_lines}")
    print(f"Tổng số ID duy nhất (không trùng lặp): {unique_ids}")

    if total_lines == TOTAL_IDS and unique_ids == TOTAL_IDS:
        print("\n=======> [THÀNH CÔNG MỸ MÃN] <=======")
        print("1. Hệ thống đã quét ĐÚNG 2 FILE CSV test.")
        print("2. Chạy tiếp sức đúng chỗ bị ngắt không sót ID nào.")
        print("3. KHÔNG có ID nào bị cào lặp lại 2 lần!")
    else:
        print("\n=======> [THẤT BẠI] <=======")
        print("Dữ liệu lưu bị sai lệch số lượng hoặc bị trùng lặp.")

    clean_test_environment()
    print("[-] Hoàn tất dọn dẹp. Môi trường làm việc đã sạch sẽ!")


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_integration_test())