"""
Query the Copernicus Data Space Ecosystem OData API

https://documentation.dataspace.copernicus.eu/APIs/OData.html
"""

import json
from dataclasses import dataclass
from datetime import date, datetime
from random import random
from time import sleep
from typing import Any, Dict, List, Union
from urllib.parse import quote

import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger


class _FeatureIterator:
    def __init__(self, feature_query) -> None:
        self.index = 0
        self.feature_query = feature_query

    def __len__(self) -> int:
        return len(self.feature_query)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            item = self.feature_query[self.index]
            self.index += 1
            return item
        except IndexError as exc:
            raise StopIteration from exc


class FeatureQuery:  # pylint: disable=too-many-instance-attributes
    """
    An iterator over the features matching the search terms

    Queries the API in batches (default: 1000), and returns them one by one.
    Queries the next batch when the current batch is exhausted.

    Note: OData API has a hard limit of 10,000 results per query
    due to $skip limitation.
    """

    total_results: int = -1
    _skip_count: int = 0
    _top: int = 1000

    def __init__(
        self,
        collection: str,
        search_terms: Dict[str, Any],
        proxies: Union[Dict[str, str], None] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> None:
        self.features = []
        self.proxies = proxies
        self.log = (options or {}).get("logger") or NoopLogger()
        self.collection = collection
        self.search_terms = search_terms

        # Option to expand Attributes for product metadata (default: True)
        self.expand_attributes = (options or {}).get("expand_attributes", True)

        self._top = search_terms.get("top", 1000)
        if self._top > 1000:
            self.log.warning("Maximum $top value is 1000, setting to 1000")
            self._top = 1000
        self.next_url = self._build_query_url(include_count=True)

    def __iter__(self):
        return _FeatureIterator(self)

    def __len__(self) -> int:
        if self.total_results < 0:
            self.__fetch_features()

        return self.total_results

    def __getitem__(self, index: int):
        while index >= len(self.features) and self.next_url is not None:
            self.__fetch_features()

        return self.features[index]

    def _build_query_url(self, include_count: bool = False) -> str:
        """Build query URL with current skip offset"""
        base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

        filter_expr = _build_odata_filter(self.collection, self.search_terms)

        params = [f"$filter={quote(filter_expr)}"]
        params.append(f"$top={self._top}")

        if self._skip_count > 0:
            params.append(f"$skip={self._skip_count}")

        if include_count:
            params.append("$count=true")

        # Optionally expand Attributes to get product metadata
        # (productType, cloudCover, etc.)
        if self.expand_attributes:
            params.append("$expand=Attributes")

        # Add ordering for consistent pagination
        params.append("$orderby=ContentDate/Start%20asc")

        return f"{base_url}?{'&'.join(params)}"

    def __fetch_features(self) -> None:
        if self.next_url is None:
            return

        if self._skip_count >= 10000:
            self.log.error(
                "Reached maximum pagination limit (10,000 results). "
                "Cannot fetch more results. Consider narrowing your search criteria "
                "(e.g., smaller date ranges)."
            )
            self.next_url = None
            return

        if self._skip_count >= 9000:
            self.log.warning(
                f"Approaching pagination limit ({self._skip_count}/10,000 results). "
                "Consider narrowing your search criteria."
            )

        session = Credentials.make_session(
            None, False, Credentials.RETRIES, self.proxies
        )
        attempts = 0
        while attempts < 10:
            attempts += 1
            try:
                with session.get(self.next_url) as response:
                    if response.status_code != 200:
                        self.log.warning(
                            f"Status code {response.status_code}, retrying.."
                        )
                        sleep(60 * (1 + (random() / 4)))
                        continue

                    odata_response = response.json()

                    products = odata_response.get("value", [])

                    self.features += products

                    if "@odata.count" in odata_response:
                        self.total_results = odata_response["@odata.count"]

                    self._skip_count += len(products)
                    self.__set_next_url(odata_response)
                    return
            except (ChunkedEncodingError, ConnectionResetError, ProtocolError) as e:
                self.log.warning(e)
                continue

    def __set_next_url(self, odata_response: Dict) -> None:
        """Set next URL from OData response"""
        # Don't follow next link if top=0 (count-only mode)
        if (
            "@odata.nextLink" in odata_response
            and self._skip_count < 10000
            and self._top > 0
        ):
            self.next_url = self._build_query_url(include_count=False)
        else:
            self.next_url = None


def query_features(
    collection: str,
    search_terms: Dict[str, Any],
    proxies: Union[Dict[str, str], None] = None,
    options: Union[Dict[str, Any], None] = None,
) -> FeatureQuery:
    """
    Returns an iterator over the features matching the search terms

    Args:
        collection: Collection name (e.g., "SENTINEL-2")
        search_terms: Dictionary of search parameters
        proxies: Optional proxy configuration
        options: Optional settings:
            - expand_attributes (bool): Include product attributes in response
              (productType, cloudCover, platform, etc.). Default: True.
              Set to False for faster queries if attributes aren't needed.

    Note: The OData API has a pagination limit of 10,000 results per query.
    If your query returns more results, consider narrowing the search criteria.
    """
    # Set default top to 1000 (OData maximum per request)
    if "top" not in search_terms:
        search_terms = {"top": 1000, **search_terms}

    return FeatureQuery(collection, search_terms, proxies, options)


def shape_to_wkt(shape: str) -> str:
    """
    Convert a shapefile to a WKT string
    """
    # pylint: disable=line-too-long
    coordinates = list(gpd.read_file(shape).geometry[0].exterior.coords)  # pyright:ignore[reportAttributeAccessIssue]
    return (
        "POLYGON(("
        + ", ".join(" ".join(map(str, coord)) for coord in coordinates)
        + "))"
    )


def geojson_to_wkt(geojson_in: Union[str, Dict]) -> str:
    """
    Convert a geojson geometry to a WKT string
    """
    geojson = json.loads(geojson_in) if isinstance(geojson_in, str) else geojson_in

    if geojson.get("type") == "Feature":
        geojson = geojson["geometry"]
    elif geojson.get("type") == "FeatureCollection" and len(geojson["features"]) == 1:
        geojson = geojson["features"][0]["geometry"]

    coordinates = str(
        tuple(item for sublist in geojson["coordinates"][0] for item in sublist)
    )
    paired_coord = ",".join(
        [
            f"{a}{b}"
            for a, b in zip(coordinates.split(",")[0::2], coordinates.split(",")[1::2])
        ]
    )
    return f"POLYGON({paired_coord})"


def describe_collection(
    collection: str, proxies: Union[Dict[str, str], None] = None
) -> Dict[str, Any]:
    """
    Get a list of valid OData filter parameters for a given collection

    Note: This is a simplified version that returns common parameters.
    The OData API does not provide a schema description endpoint like OpenSearch did.
    """
    # pylint: disable=unused-argument
    # Common parameters across all collections
    common_params = {
        "startDate": {
            "title": "Start date for acquisition (ContentDate/Start gt)",
            "pattern": r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$",
            "example": "2024-01-01 or 2024-01-01T00:00:00Z",
        },
        "startDateBefore": {
            "title": "Upper bound for start date (ContentDate/Start lt)",
            "pattern": r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$",
            "example": "2024-01-31 or 2024-01-31T23:59:59Z",
        },
        "completionDate": {
            "title": "End date for acquisition (ContentDate/End lt)",
            "pattern": r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$",
            "example": "2024-01-31 or 2024-01-31T23:59:59Z",
        },
        "geometry": {
            "title": "WKT geometry for spatial filtering",
            "example": "POLYGON((lon1 lat1, lon2 lat2, ...))",
        },
        "productType": {
            "title": "Product type (e.g., S2MSI1C, S2MSI2A, GRD, SLC)",
        },
        "orbitDirection": {
            "title": "Orbit direction (ASCENDING or DESCENDING)",
        },
        "relativeOrbitNumber": {
            "title": "Relative orbit number",
        },
    }

    # Collection-specific parameters
    collection_params = {
        "SENTINEL-2": {
            "cloudCover": {
                "title": "Maximum cloud cover percentage (0-100)",
                "maxInclusive": "100",
                "minInclusive": "0",
            },
        },
    }

    params = common_params.copy()
    if collection.upper() in collection_params:
        params.update(collection_params[collection.upper()])

    return params


@dataclass(frozen=True)
class DateFilterSpec:
    """Specification for a date-based filter."""

    odata_field: str
    operator: str


_DATE_FILTERS: Dict[str, DateFilterSpec] = {
    "startDate": DateFilterSpec("ContentDate/Start", "gt"),
    "startDateAfter": DateFilterSpec("ContentDate/Start", "gt"),
    "startDateBefore": DateFilterSpec("ContentDate/Start", "lt"),
    "completionDate": DateFilterSpec("ContentDate/End", "lt"),
    "publishedAfter": DateFilterSpec("PublicationDate", "gt"),
    "publishedBefore": DateFilterSpec("PublicationDate", "lt"),
}

# Attribute parameters grouped by type
_STRING_ATTRIBUTES = {"productType", "orbitDirection", "sensorMode", "processingLevel"}
_INTEGER_ATTRIBUTES = {"orbitNumber", "relativeOrbitNumber"}

# Parameters to skip (handled separately or not filter-related)
_INTERNAL_PARAMS = {"top", "skip"}


def _build_odata_filter(collection: str, search_terms: Dict[str, Any]) -> str:
    """Build $filter expression from search terms."""
    filters: List[str] = [f"Collection/Name eq '{collection}'"]

    for key, value in search_terms.items():
        if key in _INTERNAL_PARAMS:
            continue

        if key == "box":
            raise ValueError(
                "The 'box' parameter is no longer supported. "
                "Use 'geometry' with WKT POLYGON format instead. "
                "Example: geometry='POLYGON((west south, west north, "
                "east north, east south, west south))'. "
                "See README for conversion examples."
            )

        if key in _DATE_FILTERS:
            spec = _DATE_FILTERS[key]
            date_str = _format_odata_date(value)
            filters.append(f"{spec.odata_field} {spec.operator} {date_str}")

        elif key == "geometry":
            filters.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{value}')")

        elif key == "cloudCover":
            filters.append(_build_cloud_cover_filter(value))

        elif key in _STRING_ATTRIBUTES:
            filters.append(_build_attribute_filter(key, value, "StringAttribute", "eq"))

        elif key in _INTEGER_ATTRIBUTES:
            filters.append(
                _build_attribute_filter(key, value, "IntegerAttribute", "eq")
            )

        elif key == "raw_filter":
            filters.append(str(value))

        # Ignore unknown parameters (they may be OData-specific like orderby)

    return " and ".join(filters)


def _format_odata_date(date_value: Union[str, date, datetime]) -> str:
    """Format date value."""
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if isinstance(date_value, date):
        return f"{date_value.strftime('%Y-%m-%d')}T00:00:00.000Z"
    if isinstance(date_value, str):
        # If already formatted, return as-is
        if "T" in date_value:
            # Ensure it ends with Z if no timezone
            has_tz = (
                date_value.endswith("Z")
                or "+" in date_value
                or date_value.count("-") > 2
            )
            if not has_tz:
                return f"{date_value}.000Z"
            return date_value
        # Date-only string
        return f"{date_value}T00:00:00.000Z"
    return str(date_value)


def _build_attribute_filter(
    attr_name: str,
    attr_value: Any,
    attr_type: str,
    operator: str = "eq",
) -> str:
    """
    Build OData attribute filter expression
    """
    value_str = str(attr_value)

    if attr_type == "StringAttribute":
        value_str = f"'{attr_value}'"
    elif attr_type in ("DoubleAttribute", "IntegerAttribute"):
        # Ensure numeric format
        if attr_type == "DoubleAttribute":
            value_str = str(float(attr_value))
        else:
            value_str = str(int(attr_value))

    return (
        f"Attributes/OData.CSC.{attr_type}/any("
        f"att:att/Name eq '{attr_name}' and "
        f"att/OData.CSC.{attr_type}/Value {operator} {value_str})"
    )


def _build_attribute_range_filter(
    attr_name: str,
    min_value: Union[int, float],
    max_value: Union[int, float],
    attr_type: str,
) -> str:
    """Build attribute range filter for [min_value, max_value]."""
    # Build two separate attribute filters and join them
    min_filter = _build_attribute_filter(attr_name, min_value, attr_type, "ge")
    max_filter = _build_attribute_filter(attr_name, max_value, attr_type, "le")
    return f"({min_filter} and {max_filter})"


def _build_cloud_cover_filter(value: Any) -> str:
    """Build filter for cloudCover. Accepts single value (max) or [min, max] range."""
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _build_attribute_range_filter(
            "cloudCover", value[0], value[1], "DoubleAttribute"
        )
    return _build_attribute_filter("cloudCover", value, "DoubleAttribute", "le")


def get_product_attribute(product: Dict[str, Any], name: str) -> Any:
    """
    Get an attribute value from a product's Attributes array.

    Args:
        product: Product dictionary
        name: Attribute name to retrieve (e.g., 'cloudCover', 'productType')

    Returns:
        The attribute value if found, None otherwise
    """
    for attr in product.get("Attributes", []):
        if attr.get("Name") == name:
            return attr.get("Value")
    return None
