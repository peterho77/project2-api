import asyncio
import aiohttp
import pandas as pd
import os
from dotenv import load_dotenv
from tiki_scraper import process_chunk

# Nạp các biến môi trường từ file .env
load_dotenv()

file_path=os.getenv("CSV_FILE_PATH")
column=os.getenv("CSV_COLUMN_NAME")
chunk_size=int(os.getenv("CHUNK_SIZE"))
concurrency_limit=int(os.getenv("CONCURRENCY_LIMIT"))
api_url=os.getenv("API_URL")


async def main():
    print("Đang đọc file CSV...")
    try:
        df = pd.read_csv(file_path)
        # Loại bỏ các dòng trống (NaN), ép về số thực, rồi số nguyên, rồi mới sang chuỗi
        # Cách này an toàn để cắt bỏ hoàn toàn đuôi .0
        product_ids = df[column].dropna().apply(lambda x: str(int(float(x)))).tolist()
    except Exception as e:
        print(f"Lỗi đọc file CSV: {e}")
        return

    total_ids = len(product_ids)
    print(f"Tổng số ID cần xử lý: {total_ids}")

    # Semaphore giới hạn số luồng (coroutines) gửi đi cùng một lúc
    semaphore = asyncio.Semaphore(concurrency_limit)

    # Sử dụng duy nhất 1 ClientSession cho toàn bộ tiến trình để tái sử dụng TCP connection (Keep-Alive)
    async with aiohttp.ClientSession() as session:
        for i in range(0, total_ids, chunk_size):
            chunk = product_ids[i:i + chunk_size]
            chunk_index = (i // chunk_size) + 1
            print(f"> Đang xử lý Chunk {chunk_index}...")
            await process_chunk(session, chunk, chunk_index, semaphore)
            await asyncio.sleep(3)


if __name__ == "__main__":
    # Fix lỗi "Event loop is closed" trên Windows khi dùng asyncio
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())