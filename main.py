import asyncio
import aiohttp
import pandas as pd
import os
from dotenv import load_dotenv
from tiki_scraper import worker,file_writer

# Nạp các biến môi trường từ file .env
load_dotenv()

file_path=os.getenv("CSV_FILE")
column=os.getenv("CSV_COLUMN_NAME")
chunk_size=int(os.getenv("CHUNK_SIZE"))
concurrency_limit=int(os.getenv("CONCURRENCY_LIMIT"))
api_url=os.getenv("API_URL")
processed_file=os.getenv("PROCESSED_FILE")

def get_processed_ids():
    """Đọc danh sách các ID đã xử lý từ file txt."""
    if not os.path.exists(processed_file):
        return set()  # Dùng set() để tra cứu tốc độ O(1)

    with open(processed_file, 'r', encoding='utf-8') as f:
        # Đọc từng dòng và xóa khoảng trắng/xuống dòng
        return set(line.strip() for line in f if line.strip())

async def main():
    print("Đang đọc file CSV...")
    try:
        df = pd.read_csv(file_path)
        product_ids = df[column].dropna().apply(lambda x: str(int(float(x)))).tolist()
    except Exception as e:
        print(f"Lỗi đọc file CSV: {e}")
        return

    # LỌC CÁC ID ĐÃ XỬ LÝ
    processed_ids = get_processed_ids()
    product_ids = [pid for pid in product_ids if pid not in processed_ids]

    print(f"Tổng số ID trong file: {len(product_ids)}")
    print(f"Đã xử lý trước đó: {len(processed_ids)}")
    print(f"Số ID cần chạy tiếp: {len(product_ids)}")

    if len(product_ids) == 0:
        print("Tất cả dữ liệu đã được cào xong!")
        return

    # Khởi tạo 2 Queue
    job_queue = asyncio.Queue()
    result_queue = asyncio.Queue()

    # Nhồi toàn bộ ID vào job_queue
    for pid in product_ids:
        job_queue.put_nowait(pid)

    connector = aiohttp.TCPConnector(limit=concurrency_limit, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        # 1. Khởi chạy duy nhất 1 task ghi file
        writer_task = asyncio.create_task(file_writer(result_queue, chunk_size=chunk_size))

        # 2. Khởi chạy 50 tasks worker tải dữ liệu
        workers = []
        for i in range(concurrency_limit):
            task = asyncio.create_task(worker(i, job_queue, result_queue, session))
            workers.append(task)

        # 3. Chờ cho đến khi TẤT CẢ ID trong job_queue được lấy và xử lý xong
        await job_queue.join()

        # 4. Chờ cho đến khi TẤT CẢ kết quả trong result_queue được ghi ra file
        await result_queue.join()

        # 5. Gửi tín hiệu (None) để báo cho file_writer biết đã hết việc và tự kết thúc
        await result_queue.put(None)
        await writer_task

        # 6. Hủy các worker đang rảnh rỗi
        for w in workers:
            w.cancel()

    print("[-] Hoàn tất tiến trình cào dữ liệu.")

if __name__ == "__main__":
    # Cài đặt Policy này để tránh lỗi "Event loop is closed" nếu bạn đang dùng Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Khởi chạy hàm main
    asyncio.run(main())