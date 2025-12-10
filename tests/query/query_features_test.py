from typing import Any

import pytest

from cdsetool.query import query_features


def _mock_describe(requests_mock: Any) -> None:
    url = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/describe.xml"
    with open(
        "tests/query/mock/sentinel_1/describe.xml", "r", encoding="utf-8"
    ) as file:
        requests_mock.get(url, text=file.read())


def _mock_sentinel_1(requests_mock: Any) -> None:
    urls = [
        (
            "tests/query/mock/sentinel_1/page_1.json",
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&exactCount=1",
        ),
        (
            "tests/query/mock/sentinel_1/page_2.json",
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&exactCount=0&page=2",
        ),
        (
            "tests/query/mock/sentinel_1/page_3.json",
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&exactCount=0&page=3",
        ),
        (
            "tests/query/mock/sentinel_1/page_4.json",
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&exactCount=0&page=4",
        ),
        (
            "tests/query/mock/sentinel_1/page_5.json",
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?maxRecords=10&exactCount=0&page=5",
        ),
    ]

    for file, url in urls:
        with open(file, "r", encoding="utf-8") as file:
            requests_mock.get(url, text=file.read())


def test_query_features_length(requests_mock: Any) -> None:
    _mock_describe(requests_mock)
    _mock_sentinel_1(requests_mock)

    query = query_features("Sentinel1", {"maxRecords": 10})

    assert len(query) == 48

    manual_count = 0
    for feature in query:
        manual_count += 1

    assert manual_count == 48


def test_query_features_bad_search_terms(requests_mock: Any) -> None:
    _mock_describe(requests_mock)
    _mock_sentinel_1(requests_mock)

    with pytest.raises(AssertionError):
        query_features("Sentinel1", {"maxRecords": 10, "bogusParam": 27})

    query_features(
        "Sentinel1",
        {"maxRecords": 10, "bogusParam": 27},
        options={"validate_search_terms": False},
    )


def test_query_features_reusable(requests_mock: Any) -> None:
    _mock_describe(requests_mock)
    _mock_sentinel_1(requests_mock)

    query = query_features("Sentinel1", {"maxRecords": 10})

    assert len(query) == len(query)
    assert len(query) == 48  # query is not exhausted after first len call

    assert list(query) == list(query)  # query is not exhausted after first iteration


def test_query_features_random_access(requests_mock: Any) -> None:
    """Test random access to products with proper lazy loading"""
    _mock_sentinel_1_odata(requests_mock)

    query = query_features("Sentinel1", {"maxRecords": 10})

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
