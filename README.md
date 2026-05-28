**📦 Tối ưu hóa requirements.txt và Quản lý Môi trường Ảo** (.venv)

**🎯 Mục tiêu**:

- Tạo ra một file requirements.txt tinh gọn và chuẩn xác nhất, chỉ chứa những thư viện thực sự được gọi (import) trong mã nguồn.
- Việc không sử dụng các lệnh gom toàn bộ thư viện cơ bản giúp dự án:
- Tránh dư thừa (Bloat): Không mang theo các thư viện rác, gói phụ thuộc chéo hoặc các công cụ chỉ dùng để test ngắn hạn.
- Hạn chế xung đột: Đảm bảo tính ổn định tuyệt đối khi chuyển giao dự án cho thành viên khác hoặc khi đưa lên server (deploy).
- Môi trường sạch sẽ: Dễ dàng kiểm soát và bảo trì các thư viện lõi của dự án.

**🔄 Quy trình chuẩn để tối ưu requirements và làm sạch môi trường**

Để đảm bảo môi trường phát triển luôn đạt trạng thái tốt nhất, mỗi khi bạn cài đặt thêm thư viện mới hoặc muốn dọn dẹp dự án, hãy thực hiện theo 3 bước sau:

**Bước 1:** Cài đặt công cụ quét mã nguồn pipreqs
Công cụ này sẽ phân tích các file .py để trích xuất chính xác các thư viện đang được sử dụng. Mở Terminal và chạy:

    pip install pipreqs

**Bước 2:** Tự động sinh file requirements.txt tối ưu
Sử dụng lệnh dưới đây để quét mã nguồn và ghi đè file requirements.txt. Tham số --ignore .venv được thêm vào để báo cho công cụ bỏ qua việc quét các file hệ thống nội bộ, giúp tăng tốc độ và độ chính xác:

    pipreqs . --ignore .venv --force

(Lưu ý: Nếu bạn sử dụng Python 3.12+ và thấy xuất hiện các dòng cảnh báo màu đỏ như SyntaxWarning: invalid escape sequence..., bạn hoàn toàn có thể phớt lờ. Đây chỉ là cảnh báo cú pháp của Python đối với mã nguồn của công cụ, file requirements.txt của bạn vẫn được tạo ra thành công và trọn vẹn).

**Bước 3:** "Đập đi xây lại" - Khởi tạo môi trường siêu sạch
Để chắc chắn môi trường hiện tại không còn tàn dư của bất kỳ thư viện rác nào, cách tốt nhất là xóa bỏ thư mục .venv cũ và cài đặt lại 100% dựa trên file requirements vừa được tối ưu.

Hãy copy và chạy chuỗi lệnh tương ứng với hệ điều hành của bạn để hệ thống tự động làm mọi việc từ A-Z:

Hãy copy và chạy chuỗi lệnh tương ứng với hệ điều hành của bạn để hệ thống tự động làm mọi việc từ A-Z:

* **Dành cho macOS / Linux:**
  ```cmd
  rm -rf .venv && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

* **Dành cho Windows (Command Prompt - cmd):**
    ```cmd
    rmdir /s /q .venv & python -m venv .venv & .venv\Scripts\activate & pip install -r requirements.txt
* **Dành cho Windows (PowerShell):**
    ```cmd
    Remove-Item -Recurse -Force .venv ; python -m venv .venv ; .venv\Scripts\Activate.ps1 ; pip install -r requirements.txt