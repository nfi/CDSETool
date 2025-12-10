"""
Query the Copernicus Data Space Ecosystem

https://documentation.dataspace.copernicus.eu/APIs/OData.html
"""

import json
from dataclasses import dataclass
from datetime import date, datetime
from random import random
from time import sleep
from typing import Any, Dict, List, Literal, Optional, Union
from urllib.parse import quote

import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from cdsetool._attributes import ATTRIBUTES
from cdsetool.credentials import Credentials
from cdsetool.logger import NoopLogger

SearchTermValue = Union[str, int, float, bool, date, datetime]

# API-imposed limits from the Copernicus OData API
MAX_BATCH_SIZE = 1000
MAX_RESULTS_LIMIT = 10000


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


def _to_int(value: SearchTermValue) -> int:
    """Convert a search term value to int, accepting only int or str."""
    if isinstance(value, (int, str)) and not isinstance(value, bool):
        return int(value)
    raise ValueError(f"Expected int or str, got {type(value).__name__}: {value!r}")


class FeatureQuery:  # pylint: disable=too-many-instance-attributes
    """An iterator over the features matching the search terms"""

    def __init__(
        self,
        collection: str,
        search_terms: Dict[str, SearchTermValue],
        proxies: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.total_results = -1
        self.features: List[Dict[str, Any]] = []
        self.proxies = proxies
        self.log = (options or {}).get("logger") or NoopLogger()
        self.collection = collection
        self.search_terms = search_terms

        # Option to expand Attributes for product metadata (default: False)
        self.expand_attributes = (options or {}).get("expand_attributes", False)

        self._skip_count = _to_int(search_terms.get("skip", 0))
        self._top = _to_int(search_terms.get("top", MAX_BATCH_SIZE))
        if self._top > MAX_BATCH_SIZE:
            self.log.warning(
                f"Maximum 'top' value is {MAX_BATCH_SIZE}, setting to {MAX_BATCH_SIZE}"
            )
            self._top = MAX_BATCH_SIZE
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
        filter_expr = _build_odata_filter(self.collection, self.search_terms)

        params = [
            f"$filter={quote(filter_expr)}",
            f"$top={self._top}",
            # Add ordering for consistent pagination
            "$orderby=ContentDate/Start%20asc",
        ]

        if self._skip_count > 0:
            params.append(f"$skip={self._skip_count}")

        if include_count:
            params.append("$count=true")

        # Optionally expand Attributes to get product metadata
        # (productType, cloudCover, etc.)
        if self.expand_attributes:
            params.append("$expand=Attributes")

        return (
            "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
            f"{'&'.join(params)}"
        )

    def __fetch_features(self) -> None:
        if self.next_url is None:
            return

        if self._skip_count >= MAX_RESULTS_LIMIT:
            self.log.error(
                "Reached maximum pagination limit (10,000 results). "
                "Cannot fetch more results. Consider narrowing your search criteria "
                "(e.g., smaller date ranges)."
            )
            self.next_url = None
            return

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

                    # In contrast to the OpenSearch API, the OData API does not
                    # include the collection name in the returned products, but
                    # the download requires it.
                    for product in products:
                        product["Collection"] = self.collection

                    self.features += products

                    if "@odata.count" in odata_response:
                        self.total_results = odata_response["@odata.count"]
                    elif self.total_results < 0:
                        self.log.error("Total result count not present in response.")

                    self._skip_count += len(products)
                    self.__set_next_url(odata_response)
                    return
            except (ChunkedEncodingError, ConnectionResetError, ProtocolError) as e:
                self.log.warning(e)
                continue

        self.log.error("Failed to fetch features after %d attempts", attempts)
        self.next_url = None

    def __set_next_url(self, odata_response: Dict) -> None:
        has_next_page = (
            self._skip_count < MAX_RESULTS_LIMIT
            and self._top > 0
            and "@odata.nextLink" in odata_response
        )
        self.next_url = (
            self._build_query_url(include_count=False) if has_next_page else None
        )


def query_features(
    collection: str,
    search_terms: Dict[str, SearchTermValue],
    proxies: Optional[Dict[str, str]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> FeatureQuery:
    """
    Returns an iterator over the features matching the search terms

    Args:
        collection: Collection name (e.g., "SENTINEL-2")
        search_terms: Dictionary of search parameters
        proxies: Optional proxy configuration
        options: Optional settings:
            - expand_attributes (bool): Include product attributes in response
              (productType, cloudCover, platform, etc.). Default: False.
              Set to True if you need to access attributes like cloudCover.

    Note: The API has a pagination limit of 10,000 results per query.
    If your query returns more results, consider narrowing the search criteria.
    """
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


@dataclass(frozen=True)
class DateFilterSpec:
    """Specification for a date-based filter."""

    odata_field: str
    operator: str
    title: str


ODataComparisonOp = Literal["eq", "lt", "le", "gt", "ge"]

_OPERATOR_LABELS = {
    "eq": "equals",
    "lt": "less than",
    "le": "less than or equal",
    "gt": "greater than",
    "ge": "greater than or equal",
}

_DATE_FIELD_SPECS: List[tuple[str, str, str]] = [
    ("startDate", "ContentDate/Start", "Acquisition start date"),
    ("completionDate", "ContentDate/End", "Acquisition end date"),
    ("published", "PublicationDate", "Publication date"),
]

_DATE_FILTERS: Dict[str, DateFilterSpec] = {}
for _base, _field, _desc in _DATE_FIELD_SPECS:
    for _suffix, _op in [
        ("", "eq"),
        ("Lt", "lt"),
        ("Le", "le"),
        ("Gt", "gt"),
        ("Ge", "ge"),
    ]:
        _DATE_FILTERS[f"{_base}{_suffix}"] = DateFilterSpec(
            _field,
            _op,
            f"{_desc} {_OPERATOR_LABELS[_op]} ({_field} {_op})",
        )

_BUILTIN_PARAMS: Dict[str, Dict[str, str]] = {
    "geometry": {
        "title": "WKT geometry for spatial filtering",
        "example": "POLYGON((lon1 lat1, lon2 lat2, ...))",
    },
}


def describe_search_terms() -> Dict[str, Dict[str, str]]:
    """Get builtin search terms (date filters, geometry) that are always available.

    Returns only the builtin parameters. To get collection-specific attributes,
    use describe_collection() with a collection name.
    """
    terms: Dict[str, Dict[str, str]] = {
        key: {"title": spec.title, "example": "2024-01-01 or 2024-01-01T00:00:00Z"}
        for key, spec in _DATE_FILTERS.items()
    }
    terms.update(_BUILTIN_PARAMS)
    return terms


def _fetch_collection_attributes(
    collection: str, proxies: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    """Fetch available attributes for a collection from the OData API."""
    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Attributes({collection})"
    session = Credentials.make_session(None, False, Credentials.RETRIES, proxies)

    try:
        response = session.get(url, timeout=30)
        if response.status_code == 404:
            raise ValueError(f"Collection '{collection}' not found")
        if response.status_code != 200:
            raise ValueError(
                f"Failed to fetch attributes for '{collection}': "
                f"HTTP {response.status_code}"
            )
        return response.json()
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Error fetching attributes for '{collection}': {e}") from e


def describe_collection(
    collection: str, proxies: Optional[Dict[str, str]] = None
) -> Dict[str, Dict[str, str]]:
    """
    Get available filter parameters for a given collection.

    Fetches available attributes from the OData API's Attributes endpoint.
    Not all server attributes might be available as search terms at this time.

    Args:
        collection: Collection name (e.g., "SENTINEL-2", "SENTINEL-1")
        proxies: Optional proxy configuration

    Returns:
        Dictionary of parameters that can be used in filters (builtin + server attrs)
    """
    # Start with built-in search terms (base date names only, no Lt/Le/Gt/Ge variants)
    search_terms: Dict[str, Dict[str, str]] = {
        base: {"title": desc, "example": "2024-01-01 or 2024-01-01T00:00:00Z"}
        for base, _field, desc in _DATE_FIELD_SPECS
    }
    search_terms.update(_BUILTIN_PARAMS)

    # Fetch attributes for the collection from the server
    try:
        server_attributes = _fetch_collection_attributes(collection, proxies)

        for attr in server_attributes:
            name = attr.get("Name")
            value_type = attr.get("ValueType", "String")

            if not name:
                continue

            # Use title from ATTRIBUTES if available
            entry: Dict[str, str] = {"type": value_type}
            if name in ATTRIBUTES and (title := ATTRIBUTES[name].get("Title")):
                entry["title"] = title
            search_terms[name] = entry

    except ValueError:
        # If API call fails, add attributes from local ATTRIBUTES that match collection
        for attr_name, attr_info in ATTRIBUTES.items():
            if collection in attr_info.get("Collections", []):
                entry = {"type": attr_info.get("Type", "String")}
                if title := attr_info.get("Title"):
                    entry["title"] = title
                search_terms[attr_name] = entry

    return dict(sorted(search_terms.items()))


_INTERNAL_PARAMS = {"top", "skip"}

ODataAttributeType = Literal[
    "StringAttribute",
    "IntegerAttribute",
    "DoubleAttribute",
    "DateTimeOffsetAttribute",
    "BooleanAttribute",
]

_TYPE_TO_ODATA_ATTR: Dict[str, ODataAttributeType] = {
    "String": "StringAttribute",
    "Integer": "IntegerAttribute",
    "Double": "DoubleAttribute",
    "DateTimeOffset": "DateTimeOffsetAttribute",
    "Boolean": "BooleanAttribute",
}

_DEPRECATED_PARAMS: Dict[str, str] = {
    "box": (
        "The 'box' parameter was only supported in the old OpenSearch API, "
        "use the 'geometry' parameter with a polygon in WKT format instead. "
        "Example: geometry='POLYGON((west south, west north, "
        "east north, east south, west south))'. "
        "See README for conversion examples."
    ),
    "publishedAfter": (
        "The 'publishedAfter' parameter has been renamed. Use 'publishedGt' instead."
    ),
    "publishedBefore": (
        "The 'publishedBefore' parameter has been renamed. Use 'publishedLt' instead."
    ),
    "maxRecords": "The 'maxRecords' parameter has been renamed. Use 'top' instead.",
}

_OPERATOR_SUFFIXES: Dict[str, ODataComparisonOp] = {
    "Lt": "lt",
    "Le": "le",
    "Gt": "gt",
    "Ge": "ge",
}


def _parse_interval(
    value: str,
) -> Optional[tuple[str, str, ODataComparisonOp, ODataComparisonOp]]:
    """Parse interval syntax like [a,b], (a,b), [a,b), (a,b].

    Returns:
        Tuple of (start_value, end_value, start_op, end_op) or None if not an interval.
        start_op is 'ge' for '[' or 'gt' for '('
        end_op is 'le' for ']' or 'lt' for ')'
    """
    value = value.strip()
    if len(value) < 3:
        return None

    start_char = value[0]
    end_char = value[-1]

    if start_char not in "[(" or end_char not in "])":
        return None

    inner = value[1:-1]

    parts = inner.split(",")
    if len(parts) != 2:
        return None

    start_value = parts[0].strip()
    end_value = parts[1].strip()

    if not start_value or not end_value:
        return None

    # Determine operators based on brackets
    start_op = "ge" if start_char == "[" else "gt"
    end_op = "le" if end_char == "]" else "lt"

    return (start_value, end_value, start_op, end_op)


def _parse_operator_suffix(key: str) -> tuple[str, ODataComparisonOp]:
    """Parse operator suffix from a key like 'cloudCoverLt'."""
    for suffix, operator in _OPERATOR_SUFFIXES.items():
        if key.endswith(suffix):
            base_name = key[: -len(suffix)]
            return (base_name, operator)
    return (key, "eq")


def _build_generic_attribute_filters(key: str, str_value: str) -> list[str]:
    """Build OData filter expression(s) for a generic attribute parameter."""
    # Check if key has operator suffix (e.g., cloudCoverLt, orbitNumberGe)
    base_name, operator = _parse_operator_suffix(key)

    if not (attr_info := ATTRIBUTES.get(base_name)):
        raise ValueError(f"The '{key}' parameter is not supported.")

    attr_type = attr_info.get("Type", "String")

    if not (odata_attr_type := _TYPE_TO_ODATA_ATTR.get(attr_type)):
        raise ValueError(
            f"Unsupported attribute type '{attr_type}' for parameter '{key}'."
        )

    # Check for interval syntax (only for numeric and date types)
    if attr_type in ("Integer", "Double", "DateTimeOffset"):
        if interval := _parse_interval(str_value):
            start_str, end_str, start_op, end_op = interval
            return [
                _build_attribute_filter(
                    base_name, start_str, odata_attr_type, start_op
                ),
                _build_attribute_filter(base_name, end_str, odata_attr_type, end_op),
            ]

    return [_build_attribute_filter(base_name, str_value, odata_attr_type, operator)]


def _build_odata_filter(
    collection: str, search_terms: Dict[str, SearchTermValue]
) -> str:
    """Build $filter expression from search terms."""
    filters = [f"Collection/Name eq '{collection}'"]

    for key, value in search_terms.items():
        if key in _INTERNAL_PARAMS:
            continue

        if key in _DEPRECATED_PARAMS:
            raise ValueError(_DEPRECATED_PARAMS[key])

        if isinstance(value, (datetime, date)):
            str_value = _format_odata_date(value)
        else:
            str_value = str(value)

        if spec := _DATE_FILTERS.get(key):
            if interval := _parse_interval(str_value):
                start_str, end_str, start_op, end_op = interval
                filters.append(f"{spec.odata_field} {start_op} {start_str}")
                filters.append(f"{spec.odata_field} {end_op} {end_str}")
                continue
            filters.append(f"{spec.odata_field} {spec.operator} {str_value}")
        elif key == "geometry":
            filters.append(
                f"OData.CSC.Intersects(area=geography'SRID=4326;{str_value}')"
            )
        else:
            filters.extend(_build_generic_attribute_filters(key, str_value))

    return " and ".join(filters)


def _format_odata_date(date_value: Union[date, datetime]) -> str:
    """Format date value for OData filter expressions"""
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return f"{date_value.strftime('%Y-%m-%d')}T00:00:00.000Z"


def _to_odata_value_str(
    value: str,
    attr_type: ODataAttributeType,
    attr_name: str,
) -> str:
    """Convert a string value to its OData string representation."""
    if attr_type == "StringAttribute":
        return f"'{value}'"
    if attr_type == "DoubleAttribute":
        return str(float(value))
    if attr_type == "IntegerAttribute":
        return str(int(value))
    if attr_type == "DateTimeOffsetAttribute":
        return value
    if attr_type == "BooleanAttribute":
        if (value := value.lower()) not in ("true", "false"):
            raise ValueError(
                f"Invalid boolean value '{value}' for attribute '{attr_name}'. "
                "Use 'true' or 'false'."
            )
        return value
    return value


def _build_attribute_filter(
    attr_name: str,
    attr_value: str,
    attr_type: ODataAttributeType,
    operator: ODataComparisonOp = "eq",
) -> str:
    value_str = _to_odata_value_str(attr_value, attr_type, attr_name)

    return (
        f"Attributes/OData.CSC.{attr_type}/any("
        f"att:att/Name eq '{attr_name}' and "
        f"att/OData.CSC.{attr_type}/Value {operator} {value_str})"
    )


def get_product_attribute(
    product: Dict[str, Any], name: str, default: Optional[Any] = None
) -> Optional[Any]:
    """
    Get an attribute value from a product's Attributes array.

    Args:
        product: Product dictionary
        name: Attribute name to retrieve (e.g., 'cloudCover', 'productType')
        default: Value to return if attribute is not found (default: None)

    Returns:
        The attribute value if found, default otherwise
    """
    for attr in product.get("Attributes", []):
        if attr.get("Name") == name:
            return attr.get("Value")
    return default
