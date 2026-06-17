import pytest
from src.utils import _read_ids_safe, _write_ids_atomic


def test_read_ids_safe_removes_glued_nulls(mock_workspace):
    """Kiểm tra Regex bóc tách ID hợp lệ khỏi rác 'null'."""
    # 1. Chuẩn bị (Arrange): Tạo file giả lập chứa rác
    fake_failed_file = mock_workspace / "failed.csv"
    fake_failed_file.write_text("nullnull215110865\n123456\nnull\nnan")

    # 2. Thực thi (Act): Gọi hàm cần test
    result = _read_ids_safe(fake_failed_file)

    # 3. Đối chiếu (Assert): Đảm bảo chỉ nhặt đúng 2 số, tự động drop rác
    assert result == {"215110865", "123456"}
    assert "null" not in result


def test_write_ids_atomic(mock_workspace):
    """Kiểm tra cơ chế Atomic Write hoạt động an toàn."""
    target_file = mock_workspace / "dead.csv"
    test_data = {"11111", "22222"}

    _write_ids_atomic(target_file, test_data)

    # Đảm bảo file được tạo thành công và không còn file .tmp tồn đọng
    assert target_file.exists()
    assert not target_file.with_suffix('.tmp').exists()

    # Kiểm tra nội dung file có chứa \n
    content = target_file.read_text().splitlines()
    assert len(content) == 2
    assert "11111" in content