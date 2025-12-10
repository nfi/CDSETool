from datetime import date, datetime

import pytest

from cdsetool.query import (
    _build_attribute_filter,
    _build_attribute_range_filter,
    _build_odata_filter,
    _format_odata_date,
    geojson_to_wkt,
    shape_to_wkt,
)


def test_format_odata_date() -> None:
    """Test OData date formatting"""
    # datetime objects
    assert (
        _format_odata_date(datetime(2020, 1, 1, 12, 0, 0)) == "2020-01-01T12:00:00.000Z"
    )
    # date objects
    assert _format_odata_date(date(2020, 1, 1)) == "2020-01-01T00:00:00.000Z"
    # string dates
    assert _format_odata_date("2020-01-01") == "2020-01-01T00:00:00.000Z"
    # string datetimes with Z
    assert _format_odata_date("2020-01-01T12:00:00Z") == "2020-01-01T12:00:00Z"
    # string datetimes without Z
    assert _format_odata_date("2020-01-01T12:00:00") == "2020-01-01T12:00:00.000Z"


def test_build_attribute_filter() -> None:
    """Test OData attribute filter building"""
    # String attribute
    result = _build_attribute_filter("productType", "S2MSI2A", "StringAttribute", "eq")
    assert "Attributes/OData.CSC.StringAttribute/any(" in result
    assert "att/Name eq 'productType'" in result
    assert "att/OData.CSC.StringAttribute/Value eq 'S2MSI2A'" in result

    # Double attribute
    result = _build_attribute_filter("cloudCover", 40.5, "DoubleAttribute", "le")
    assert "Attributes/OData.CSC.DoubleAttribute/any(" in result
    assert "att/Name eq 'cloudCover'" in result
    assert "att/OData.CSC.DoubleAttribute/Value le 40.5" in result

    # Integer attribute
    result = _build_attribute_filter(
        "relativeOrbitNumber", 123, "IntegerAttribute", "eq"
    )
    assert "Attributes/OData.CSC.IntegerAttribute/any(" in result
    assert "att/Name eq 'relativeOrbitNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value eq 123" in result


def test_build_attribute_range_filter() -> None:
    """Test OData attribute range filter building"""
    # Double attribute range
    result = _build_attribute_range_filter("cloudCover", 10, 22, "DoubleAttribute")
    assert "Attributes/OData.CSC.DoubleAttribute/any(" in result
    assert "att/Name eq 'cloudCover'" in result
    assert "att/OData.CSC.DoubleAttribute/Value ge 10.0" in result
    assert "att/OData.CSC.DoubleAttribute/Value le 22.0" in result

    # Integer attribute range
    result = _build_attribute_range_filter(
        "relativeOrbitNumber", 50, 100, "IntegerAttribute"
    )
    assert "Attributes/OData.CSC.IntegerAttribute/any(" in result
    assert "att/Name eq 'relativeOrbitNumber'" in result
    assert "att/OData.CSC.IntegerAttribute/Value ge 50" in result
    assert "att/OData.CSC.IntegerAttribute/Value le 100" in result


def test_build_odata_filter() -> None:
    """Test full OData filter expression building"""
    # Simple filter
    result = _build_odata_filter(
        "SENTINEL-2", {"startDate": "2020-01-01", "completionDate": "2020-01-10"}
    )
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "ContentDate/Start gt 2020-01-01T00:00:00.000Z" in result
    assert "ContentDate/End lt 2020-01-10T00:00:00.000Z" in result

    # Filter with attributes
    result = _build_odata_filter(
        "SENTINEL-2", {"cloudCover": 40, "productType": "S2MSI2A"}
    )
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "cloudCover" in result
    assert "productType" in result

    # Filter with cloudCover range
    result = _build_odata_filter("SENTINEL-2", {"cloudCover": [10, 22]})
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "cloudCover" in result
    assert "Value ge 10.0" in result
    assert "Value le 22.0" in result

    # Filter with cloudCover single value (backward compatibility)
    result = _build_odata_filter("SENTINEL-2", {"cloudCover": 30})
    assert "Collection/Name eq 'SENTINEL-2'" in result
    assert "cloudCover" in result
    assert "Value le 30.0" in result
    assert "Value ge" not in result  # Should not have minimum


def test_shape_to_wkt() -> None:
    wkt = "POLYGON((10.172406299744779 55.48259118004532, 10.172406299744779 55.38234270718456, 10.42371976928382 55.38234270718456, 10.42371976928382 55.48259118004532, 10.172406299744779 55.48259118004532))"
    assert shape_to_wkt("tests/shape/POLYGON.shp") == wkt


def test_geojson_to_wkt() -> None:
    wkt = "POLYGON((10.172406299744779 55.48259118004532, 10.172406299744779 55.38234270718456, 10.42371976928382 55.38234270718456, 10.42371976928382 55.48259118004532, 10.172406299744779 55.48259118004532))"
    geojson = '{ "type": "Feature", "properties": { }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 10.172406299744779, 55.482591180045318 ], [ 10.172406299744779, 55.382342707184563 ], [ 10.423719769283821, 55.382342707184563 ], [ 10.423719769283821, 55.482591180045318 ], [ 10.172406299744779, 55.482591180045318 ] ] ] } }'

    assert geojson_to_wkt(geojson) == wkt

    geojson = '{ "type": "Polygon", "coordinates": [ [ [ 10.172406299744779, 55.482591180045318 ], [ 10.172406299744779, 55.382342707184563 ], [ 10.423719769283821, 55.382342707184563 ], [ 10.423719769283821, 55.482591180045318 ], [ 10.172406299744779, 55.482591180045318 ] ] ] }'

    assert geojson_to_wkt(geojson) == wkt

    wkt = "POLYGON((17.58127378553624 59.88489715357605, 17.58127378553624 59.80687027682205, 17.73996723627809 59.80687027682205, 17.73996723627809 59.88489715357605, 17.58127378553624 59.88489715357605))"
    geojson = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{ },"geometry":{"coordinates":[[[17.58127378553624,59.88489715357605],[17.58127378553624,59.80687027682205],[17.73996723627809,59.80687027682205],[17.73996723627809,59.88489715357605],[17.58127378553624,59.88489715357605]]],"type":"Polygon" } } ] }'

    assert geojson_to_wkt(geojson) == wkt
