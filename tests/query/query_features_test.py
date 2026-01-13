from typing import Any

import pytest

from cdsetool.query import query_features


def _mock_sentinel_1_odata(requests_mock: Any) -> None:
    """Mock OData API responses for SENTINEL-1 queries"""
    # Page 1 with $count=true
    with open(
        "tests/query/mock/sentinel_1/odata_page_1.json", "r", encoding="utf-8"
    ) as f:
        requests_mock.get(
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name%20eq%20%27SENTINEL-1%27&$top=10&$count=true&$expand=Attributes&$orderby=ContentDate/Start%20asc",
            text=f.read(),
        )

    # Subsequent pages without $count
    pages = [
        (2, 10, "odata_page_2.json"),
        (3, 20, "odata_page_3.json"),
        (4, 30, "odata_page_4.json"),
        (5, 40, "odata_page_5.json"),
    ]

    for page_num, skip, filename in pages:
        with open(
            f"tests/query/mock/sentinel_1/{filename}", "r", encoding="utf-8"
        ) as f:
            requests_mock.get(
                f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name%20eq%20%27SENTINEL-1%27&$top=10&$skip={skip}&$expand=Attributes&$orderby=ContentDate/Start%20asc",
                text=f.read(),
            )


def test_query_features_length(requests_mock: Any) -> None:
    """Test that query returns correct total count and iterates through all features"""
    _mock_sentinel_1_odata(requests_mock)
    query = query_features("SENTINEL-1", {"top": 10})

    assert len(query) == 47

    manual_count = 0
    for feature in query:
        manual_count += 1

    assert manual_count == 47


def test_query_features_unknown_params_ignored(requests_mock: Any) -> None:
    """Test that unknown parameters are ignored (no validation in OData)"""
    _mock_sentinel_1_odata(requests_mock)

    # Unknown parameters should be ignored, not cause errors
    query = query_features("SENTINEL-1", {"top": 10, "bogusParam": 27})
    # Should still work and return results
    assert len(query) == 47


def test_query_features_reusable(requests_mock: Any) -> None:
    """Test that query can be iterated multiple times"""
    _mock_sentinel_1_odata(requests_mock)

    query = query_features("SENTINEL-1", {"top": 10})

    assert len(query) == len(query)
    assert len(query) == 47  # query is not exhausted after first len call

    assert list(query) == list(query)  # query is not exhausted after first iteration


def test_query_features_random_access(requests_mock: Any) -> None:
    """Test random access to products with proper lazy loading"""
    _mock_sentinel_1_odata(requests_mock)

    query = query_features("SENTINEL-1", {"top": 10})

    # OData format: products have "Name" field directly (not nested in properties)
    assert query[0]["Name"] == "S1A_AUX_INS_V20140406T010000_G20140409T142540.SAFE"
    assert len(query.features) == 10
    assert query[9]["Name"] == "S1A_AUX_PP2_V20140406T133000_G20241125T134251.SAFE"
    assert len(query.features) == 10
    assert query[13]["Name"] == "S1A_AUX_INS_V20140406T133000_G20211028T132414.SAFE"
    assert len(query.features) == 20
    assert query[2]["Name"] == "S1A_AUX_PP2_V20140406T133000_G20251021T105030.SAFE"
    assert len(query.features) == 20
    assert (
        query[34]["Name"]
        == "S1A_OPER_AUX_PROQUA_POD__20210408T165229_V20140409T235944_20140410T235943"
    )
    assert len(query.features) == 40
