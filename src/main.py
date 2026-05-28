import time
import re
import asyncio
import aiohttp
import pandas as pd
import os

# import from folder
from config.settings import config
from src.api_client import fetch_product_info
from src.json_handler import file_writer

# config variable
file_path = config.get("CSV_FILE")
column = config.get("CSV_COLUMN_NAME")
chunk_size = int(config.get("CHUNK_SIZE", 1000))
concurrency_limit = int(config.get("CONCURRENCY_LIMIT"))
api_url = config.get("API_URL")
success_file = config.get("SUCCESS_FILE")
failed_file = config.get("FAILED_FILE")

def get_handled_ids(file_paths):
    """
    Đọc danh sách các ID đã xử lý từ nhiều file (cả thành công và thất bại).
    Dùng set() để đảm bảo tốc độ tra cứu O(1) và tự động loại bỏ ID trùng lặp.
    """
    handled = set()

    for path in file_paths:
        if path and os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                # Hàm update() sẽ nhồi toàn bộ ID đọc được vào set hiện tại
                handled.update(line.strip() for line in f if line.strip())

    return handled

async def worker(worker_id, job_queue, result_queue, session):
    """Worker lấy ID từ job_queue, tải dữ liệu, và đẩy vào result_queue."""
    while True:
        product_id = await job_queue.get()
        try:
            # Gọi hàm fetch_product_info từ file src/api_client.py
            result = await fetch_product_info(session, product_id)
            await result_queue.put((product_id, result))
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[!] Lỗi ở worker {worker_id} ID {product_id}: {e}")
        finally:
            job_queue.task_done()

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

    # 2. TÌM VÀ LỌC FILE THEO REGEX TRONG THƯ MỤC DATA/INPUT/
    input_dir = os.path.join("data","input")  # Dùng os.path.join để tự động xử lý dấu gạch chéo (/ hoặc \) tuỳ hệ điều hành

    # Kiểm tra xem thư mục có tồn tại không trước khi quét
    if not os.path.exists(input_dir):
        print(f"[!] Lỗi: Không tìm thấy thư mục '{input_dir}'. Vui lòng tạo thư mục và chép file vào.")
        return

    file_pattern = os.getenv("CSV_FILE_PATTERN", r'^products\d+\.csv$')
    pattern = re.compile(file_pattern)

    # Quét trong thư mục input_dir và lưu lại ĐƯỜNG DẪN ĐẦY ĐỦ của file
    matched_files = []
    for f in os.listdir(input_dir):
        if pattern.match(f):
            # Nối thư mục và tên file lại với nhau (VD: data/input/products1.csv)
            full_path = os.path.join(input_dir, f)
            if os.path.isfile(full_path):  # Đảm bảo nó là file chứ không phải thư mục con
                matched_files.append(full_path)

    # 3. SẮP XẾP FILE THEO SỐ TỰ NHIÊN (Tránh lỗi products10 đứng trước products2)
    def extract_number(filename):
        match = re.search(r'\d+', filename)
        return int(match.group()) if match else 0

    matched_files.sort(key=extract_number)

    # 4. CHỌN N FILE ĐẦU TIÊN THEO YÊU CẦU
    selected_files = matched_files[:n_files]

    if not selected_files:
        print("[!] Không tìm thấy file nào khớp với định dạng 'products*.csv' trong thư mục.")
        return

    print(f"[*] Đã chọn {len(selected_files)} file để xử lý: {selected_files}")

    # 5. GOM DỮ LIỆU TỪ TẤT CẢ CÁC FILE INPUT
    all_product_ids = []
    for file in selected_files:
        try:
            df = pd.read_csv(file)
            ids = df[column].dropna().apply(lambda x: str(int(float(x)))).tolist()
            all_product_ids.extend(ids)
        except Exception as e:
            print(f"[!] Lỗi đọc file {file}: {e}")

    # Lọc trùng lặp file gốc
    all_product_ids = list(dict.fromkeys(all_product_ids))

    # 6. GIAI ĐOẠN 1: CHUẨN BỊ DỮ LIỆU MỚI
    processed_ids = get_handled_ids([success_file,failed_file])
    new_ids = [pid for pid in all_product_ids if pid not in processed_ids]

    print(f"\n--- GIAI ĐOẠN 1: CÀO DỮ LIỆU MỚI ---")
    print(f"Tổng số ID trong {len(selected_files)} file input: {len(all_product_ids)}")
    print(f"Đã xử lý thành công trước đó: {len(processed_ids)}")
    print(f"Số ID mới cần cào: {len(new_ids)}")
    print(f"------------------------------------\n")

    connector = aiohttp.TCPConnector(limit=concurrency_limit, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:

        # --- HÀM TÁI SỬ DỤNG ĐỂ CHẠY TIẾN TRÌNH ---
        async def run_engine(ids_list, phase_name):
            if not ids_list:
                print(f"[*] {phase_name}: Không có ID nào cần chạy.")
                return

            print(f"[*] Bắt đầu {phase_name} với {len(ids_list)} ID...")
            job_queue = asyncio.Queue()
            result_queue = asyncio.Queue()

            # Nhồi ID vào queue
            for pid in ids_list:
                job_queue.put_nowait(pid)

            writer_task = asyncio.create_task(file_writer(result_queue, chunk_size=chunk_size))
            workers = []
            for i in range(concurrency_limit):
                task = asyncio.create_task(worker(i, job_queue, result_queue, session))
                workers.append(task)

            try:
                await job_queue.join()
                await result_queue.join()
                await result_queue.put(None)
                await writer_task
            except asyncio.CancelledError:
                print(f"\n[Cảnh báo] {phase_name} bị ngắt! Đang dọn dẹp...")
                raise
            finally:
                for w in workers:
                    if not w.done():
                        w.cancel()
                if not writer_task.done():
                    writer_task.cancel()
            print(f"[-] Hoàn tất {phase_name}.")

        # -----------------------------------------

        # THỰC THI GIAI ĐOẠN 1
        await run_engine(new_ids, "GIAI ĐOẠN 1 (File Data Mới)")

        print(f"\n--- GIAI ĐOẠN 2: CHẠY LẠI CÁC ID LỖI (RETRY) ---")

        # QUAN TRỌNG: Cần đọc lại 2 file này từ ổ cứng vì Giai đoạn 1 vừa ghi thêm data vào chúng
        updated_processed_ids = get_handled_ids([processed_file])
        failed_ids = get_handled_ids([failed_file])

        # Lấy những ID nằm trong failed_ids nhưng chưa có trong danh sách thành công
        retry_ids = [pid for pid in failed_ids if pid not in updated_processed_ids]

        print(f"Số ID thất bại cần chạy lại: {len(retry_ids)}")
        print(f"------------------------------------------------\n")

        # THỰC THI GIAI ĐOẠN 2
        await run_engine(retry_ids, "GIAI ĐOẠN 2 (File Data Lỗi)")

    print("\n[====== HOÀN TẤT TOÀN BỘ TIẾN TRÌNH ======]")

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