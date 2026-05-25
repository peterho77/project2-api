import time
import re
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
    # 1. NHẬP SỐ LƯỢNG FILE MUỐN CHẠY
    try:
        n_files = int(input("[?] Nhập số lượng file CSV muốn chạy (VD: 2): "))
        if n_files <= 0:
            print("[!] Số lượng file phải lớn hơn 0.")
            return
    except ValueError:
        print("[!] Lỗi: Vui lòng nhập một số nguyên hợp lệ!")
        return

    print("\nĐang quét tìm các file CSV...")

    # 2. TÌM VÀ LỌC FILE THEO REGEX
    file_pattern = os.getenv("CSV_FILE_PATTERN", r'^products\d+\.csv$')
    pattern = re.compile(file_pattern)
    matched_files = [f for f in os.listdir('.') if pattern.match(f)]

    # 3. SẮP XẾP FILE THEO SỐ TỰ NHIÊN (Tránh lỗi products10 đứng trước products2)
    def extract_number(filename):
        match = re.search(r'\d+', filename)
        return int(match.group()) if match else 0

    matched_files.sort(key=extract_number)

    # 4. CHỌN N FILE ĐẦU TIÊN THEO YÊU CẦU
    selected_files = matched_files[:n_files]

    if not selected_files:
        print("[!] Không tìm thấy file nào khớp với định dạng 'product*.csv' trong thư mục.")
        return

    print(f"[*] Đã chọn {len(selected_files)} file để xử lý: {selected_files}")

    # 5. GOM DỮ LIỆU TỪ TẤT CẢ CÁC FILE ĐÃ CHỌN
    all_product_ids = []
    for file in selected_files:
        try:
            df = pd.read_csv(file)
            ids = df[column].dropna().apply(lambda x: str(int(float(x)))).tolist()
            all_product_ids.extend(ids)
        except Exception as e:
            print(f"[!] Lỗi đọc file {file}: {e}")

    # Lọc ID trùng lặp nếu lỡ có 1 sản phẩm nằm ở 2 file CSV khác nhau
    all_product_ids = list(dict.fromkeys(all_product_ids))

    # 6. LỌC CÁC ID ĐÃ ĐƯỢC CÀO TỪ TRƯỚC (CHECKPOINT)
    processed_ids = get_processed_ids()
    product_ids = [pid for pid in all_product_ids if pid not in processed_ids]

    print(f"\n--- THỐNG KÊ DỮ LIỆU ---")
    print(f"Tổng số ID trong {len(selected_files)} file: {len(all_product_ids)}")
    print(f"Đã xử lý trước đó: {len(processed_ids)}")
    print(f"Số ID thực tế cần chạy tiếp: {len(product_ids)}")
    print(f"------------------------\n")

    if len(product_ids) == 0:
        print("Tất cả dữ liệu trong các file này đã được cào xong!")
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

        try:
            # 3. Chờ cho đến khi TẤT CẢ ID trong job_queue được lấy và xử lý xong
            await job_queue.join()

            # 4. Chờ cho đến khi TẤT CẢ kết quả trong result_queue được ghi ra file
            await result_queue.join()

            # 5. Gửi tín hiệu (None) để báo cho file_writer biết đã hết việc và tự kết thúc
            await result_queue.put(None)
            await writer_task

        except asyncio.CancelledError:
            # Bắt tín hiệu khi tiến trình bị ngắt đột ngột (do Ctrl+C hoặc file test)
            print("\n[Cảnh báo] Tiến trình bị ngắt! Đang dọn dẹp bộ nhớ...")
            raise

        finally:
            # QUAN TRỌNG NHẤT: Tiêu diệt toàn bộ worker và writer dở dang
            for w in workers:
                if not w.done():
                    w.cancel()

            if not writer_task.done():
                writer_task.cancel()

    print("[-] Hoàn tất tiến trình cào dữ liệu.")

if __name__ == "__main__":
    # Cài đặt Policy này để tránh lỗi "Event loop is closed" nếu bạn đang dùng Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    start_time = time.time()

    try:
        # Khởi chạy toàn bộ logic chính
        asyncio.run(main())

    except KeyboardInterrupt:
        # Bắt trường hợp bạn bấm dừng bằng phím (Ctrl + C)
        print("\n[!] Đã bấm dừng bằng tay (Ctrl+C). Tiến trình tạm nghỉ an toàn.")

    except Exception as e:
        # Bắt tất cả các lỗi ngẫu nhiên khác (mất mạng, lỗi API văng code,...)
        print(f"\n[!] Tiến trình dừng đột ngột do lỗi: {e}")

    finally:
        end_time = time.time()
        elapsed_seconds = end_time - start_time

        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        print("==================================================")
        print(f"[*] Thời gian phiên chạy này: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print("==================================================")