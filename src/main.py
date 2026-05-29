import time
import re
import asyncio
import aiohttp
import pandas as pd
import os
import logging
import sys

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
num_files_to_run = config.get("NUM_FILES_TO_RUN",0)

# Lấy thư mục chứa file code hiện tại (thư mục 'src')
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Lùi ra một cấp để về thư mục gốc của project
BASE_DIR = os.path.dirname(CURRENT_DIR)

#logging
output_dir = os.path.join(BASE_DIR,"data", "output")
os.makedirs(output_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(output_dir, 'report.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)


def get_handled_ids(file_names, base_dir):
    """
    Đọc danh sách các ID đã xử lý từ nhiều file (cả thành công và thất bại) trong thư mục data/output/.
    Dùng set() để đảm bảo tốc độ tra cứu O(1) và tự động loại bỏ ID trùng lặp.
    """
    handled = set()

    # 1. Tạo đường dẫn tuyệt đối trỏ thẳng đến thư mục data/output/
    output_dir = os.path.join(base_dir, "data", "output")

    for file_name in file_names:
        if file_name:
            # 2. Ghép nối để tạo đường dẫn tuyệt đối đến từng file cụ thể
            path = os.path.join(output_dir, file_name)

            # 3. Kiểm tra xem file có tồn tại không trước khi đọc
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    # Hàm update() sẽ nhồi toàn bộ ID đọc được vào set hiện tại
                    handled.update(line.strip() for line in f if line.strip())
            else:
                print(f"[*] Chưa có file tracking: {path} (Sẽ tự động tạo sau)")

    return handled


async def progress_reporter(progress_dict, total_ids):
    """Task chạy ngầm để in tiến độ ra màn hình mỗi 0.5 giây."""
    try:
        while progress_dict["done"] < total_ids:
            done = progress_dict["done"]
            percent = (done / total_ids) * 100 if total_ids > 0 else 0

            # Ký tự \r giúp ghi đè lên dòng hiện tại.
            # Dùng khoảng trắng thừa ở cuối để xóa sạch các ký tự cũ dài hơn nếu có.
            sys.stdout.write(f"\r[>] Tiến độ đang chạy: {done}/{total_ids} ID ({percent:.2f}%)    ")
            sys.stdout.flush()

            await asyncio.sleep(0.5)

    except asyncio.CancelledError:
        pass  # Bỏ qua khi tiến trình bị ngắt
    finally:
        # In chốt hạ 100% khi vòng lặp kết thúc và xuống dòng (\n)
        if total_ids > 0:
            sys.stdout.write(f"\r[>] Tiến độ: {total_ids}/{total_ids} ID (100.00%) - HOÀN TẤT!          \n")
            sys.stdout.flush()

async def worker(worker_id, job_queue, result_queue, session, error_summary, progress_dict):
    """Worker lấy ID từ job_queue, tải dữ liệu, và đẩy vào result_queue."""
    while True:
        product_id = await job_queue.get()
        try:
            result = await fetch_product_info(session, product_id)

            # Kiểm tra xem kết quả trả về có phải là dict báo lỗi hay không
            if result and isinstance(result, dict) and "error" in result:
                # Phân loại lỗi và đếm số lượng
                status = result.get("status_code", "UNKNOWN")
                error_summary[status] = error_summary.get(status, 0) + 1

                # Vẫn ném vào result_queue (với giá trị None) để file_writer ghi ID này vào failed_ids.csv
                await result_queue.put((product_id, None))

            else:
                # Nếu thành công, trả dữ liệu vào queue để lưu JSON
                await result_queue.put((product_id, result))

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[!] Lỗi crash ở worker {worker_id} ID {product_id}: {e}")
            error_summary["FATAL_ERROR"] = error_summary.get("FATAL_ERROR", 0) + 1
            await result_queue.put((product_id, None))
        finally:
            # DÙ THÀNH CÔNG HAY THẤT BẠI, CỨ XONG 1 ID LÀ CỘNG LÊN 1
            progress_dict["done"] += 1
            job_queue.task_done()

async def main():
    # 1. NHẬP SỐ LƯỢNG FILE MUỐN CHẠY
    print("\nĐang quét tìm các file CSV...")

    # 2. TÌM VÀ LỌC FILE THEO REGEX TRONG THƯ MỤC DATA/INPUT/
    input_dir = os.path.join(BASE_DIR, "data", "input")  # Dùng os.path.join để tự động xử lý dấu gạch chéo (/ hoặc \) tuỳ hệ điều hành

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
    def extract_number(filepath):
        # Tách riêng tên file (VD: 'products1.csv') khỏi đường dẫn dài
        filename = os.path.basename(filepath)
        # Bây giờ re.search sẽ chỉ quét trên chuỗi 'products1.csv'
        match = re.search(r'\d+', filename)
        return int(match.group()) if match else 0

    matched_files.sort(key=extract_number)
    total_available_files = len(matched_files)

    if total_available_files == 0:
        print("[!] Không tìm thấy file nào khớp với định dạng 'products*.csv' trong thư mục.")
        return

    # 2. LOGIC HYBRID: XÁC ĐỊNH SỐ LƯỢNG FILE CẦN CHẠY
    n_files = total_available_files  # Mặc định ban đầu là quét TẤT CẢ

    # Kiểm tra xem có người đang ngồi ở Terminal gõ phím hay không
    if sys.stdin.isatty():
        # Option 1: CHẠY TAY (Manual Mode)
        try:
            user_input = input(
                f"[?] Tìm thấy {total_available_files} file. Nhập số file muốn chạy (Nhấn Enter để chạy TẤT CẢ): ")
            if user_input.strip():  # Nếu người dùng có gõ một con số
                n_files = int(user_input)
                if n_files <= 0:
                    print("[!] Số lượng file phải lớn hơn 0.")
                    return
        except ValueError:
            print("[!] Lỗi: Vui lòng nhập một số nguyên hợp lệ!")
            return
    else:
        # Option 2: CHẠY TỰ ĐỘNG BẰNG SYSTEMD (Auto Mode)
        # Bỏ qua input(), tự động đọc biến môi trường, nếu không cấu hình thì chạy ALL
        input_file_limit = int(num_files_to_run)
        if input_file_limit > 0:
            try:
                n_files = int(input_file_limit)
            except ValueError:
                pass  # Nếu cấu hình sai thì cứ giữ nguyên mặc định là quét TẤT CẢ

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
    processed_ids = get_handled_ids([success_file,failed_file],BASE_DIR)
    new_ids = [pid for pid in all_product_ids if pid not in processed_ids]

    print(f"\n--- GIAI ĐOẠN 1: CÀO DỮ LIỆU MỚI ---")
    print(f"Tổng số ID trong {len(selected_files)} file input: {len(all_product_ids)}")
    print(f"Đã xử lý thành công trước đó: {len(processed_ids)}")
    print(f"Số ID mới cần cào: {len(new_ids)}")
    print(f"------------------------------------\n")

    connector = aiohttp.TCPConnector(limit=concurrency_limit, ttl_dns_cache=300)

    # 1. TẠO "GIỎ" CHỨA THỐNG KÊ LỖI
    error_summary = {}

    async with aiohttp.ClientSession(connector=connector) as session:

        # --- HÀM TÁI SỬ DỤNG ĐỂ CHẠY TIẾN TRÌNH ---
        async def run_engine(ids_list, phase_name):
            total_ids = len(ids_list)
            if not ids_list:
                print(f"[*] {phase_name}: Không có ID nào cần chạy.")
                return

            print(f"[*] Bắt đầu {phase_name} với {len(ids_list)} ID...")
            job_queue = asyncio.Queue()
            result_queue = asyncio.Queue()

            # TẠO TÚI ĐẾM TIẾN ĐỘ BẮT ĐẦU TỪ 0
            progress_dict = {"done": 0}

            # Nhồi ID vào queue
            for pid in ids_list:
                job_queue.put_nowait(pid)

            writer_task = asyncio.create_task(file_writer(result_queue, chunk_size=chunk_size))
            # KHỞI CHẠY THƯ KÝ BÁO CÁO TIẾN ĐỘ
            reporter_task = asyncio.create_task(progress_reporter(progress_dict, total_ids))
            workers = []
            for i in range(concurrency_limit):
                task = asyncio.create_task(worker(i, job_queue, result_queue, session, error_summary, progress_dict))
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
                reporter_task.cancel()
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
        updated_success_ids = get_handled_ids([success_file],BASE_DIR)
        updated_failed_ids = get_handled_ids([failed_file],BASE_DIR)

        # Lấy những ID nằm trong failed_ids nhưng chưa có trong danh sách thành công
        retry_ids = [pid for pid in updated_failed_ids if pid not in updated_success_ids]

        print(f"Số ID thất bại cần chạy lại: {len(retry_ids)}")
        print(f"------------------------------------------------\n")

        # THỰC THI GIAI ĐOẠN 2
        await run_engine(retry_ids, "GIAI ĐOẠN 2 (File Data Lỗi)")

    print("\n[====== HOÀN TẤT TOÀN BỘ TIẾN TRÌNH ======]")

    if not error_summary:
        print("[+] Tuyệt vời! 100% dữ liệu được tải thành công, không có ID nào bị lỗi.")
        logging.info("Báo cáo: Hoàn thành 100% không có lỗi.")
    else:
        for status, count in error_summary.items():
            # Chuyển đổi mã lỗi thành thông điệp dễ hiểu
            if status == 404:
                msg = f"Lỗi 404 (Sản phẩm không tồn tại / Bị xóa): {count} ID"
            elif status == 429:
                msg = f"Lỗi 429 (Bị chặn do quá tải / Rate Limit): {count} ID"
            elif status == "EXHAUSTED":
                msg = f"Lỗi Timeout (Hết kiên nhẫn sau 3 lần thử lại): {count} ID"
            elif status == "FATAL_ERROR":
                msg = f"Lỗi Crash ngoại lệ (Dữ liệu dị dạng): {count} ID"
            else:
                msg = f"Lỗi HTTP {status} (Lỗi máy chủ Tiki hoặc mạng): {count} ID"

            print(f"  -> {msg}")
            logging.info(msg)  # Ghi bảng sao kê này vào file report.log luôn
    print("============================================\n")

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