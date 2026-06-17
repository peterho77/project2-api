import pytest
import asyncio
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_worker_handles_cancelled_error(mocker):
    """Test worker thoát êm ái, không bị task_done crash khi bị ngắt ngang."""
    # Giả lập Queue và Worker
    mock_job_queue = asyncio.Queue()
    mock_result_queue = asyncio.Queue()

    # Ép Queue ném lỗi CancelledError ngay khi worker cố lấy việc
    mocker.patch.object(mock_job_queue, 'get', side_effect=asyncio.CancelledError)

    from main import worker
    # Chạy worker, mong đợi nó thoát êm ái bằng lệnh break mà không throw error
    await worker(1, mock_job_queue, mock_result_queue, None, {}, {})

    # Khẳng định task_done() không bị gọi oan do đã ngắt ngang ở try block đầu tiên
    assert mock_job_queue.qsize() == 0