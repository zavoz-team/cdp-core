import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        path = Path(str(item.fspath))
        if 'repository' in path.parts:
            item.add_marker(pytest.mark.integration)
            continue

        item.add_marker(pytest.mark.unit)
