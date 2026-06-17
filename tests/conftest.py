import pytest
from pathlib import Path

@pytest.fixture
def mock_workspace(tmp_path: Path):
    """Tạo không gian thư mục ảo cho việc đọc/ghi file."""
    input_dir = tmp_path / "data" / "input"
    output_dir = tmp_path / "data" / "output"

    input_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    return output_dir