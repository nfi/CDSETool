from unittest.mock import MagicMock

import pytest

from cdsetool.download import download_product
from cdsetool.logger import NoopLogger


def test_noop_logger_is_default() -> None:
    NoopLogger.debug = MagicMock()

    assert NoopLogger.debug.call_count == 0

    # OData format product - missing Id will cause bad URL, triggering debug log
    download_product(
        {
            "bad_object": True,
            "Name": "myfile.xml",
            # Missing Id will cause bad URL
        },
        "somewhere",
    )

    assert NoopLogger.debug.call_count == 1


def test_noop_does_not_error() -> None:
    try:
        # OData format product - missing Id will cause bad URL, triggering debug log
        download_product(
            {
                "bad_object": True,
                "Name": "myfile.xml",
                # Missing Id will cause bad URL
            },
            "somewhere",
        )
        NoopLogger().debug("NoopLogger did not raise an exception")
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")
