"""
Command line interface
"""

import json as JSON
import os
import sys
from typing import Any, Dict, List, Optional

import typer
from typing_extensions import Annotated

from cdsetool.download import download_features, get_product_download_info
from cdsetool.monitor import StatusMonitor
from cdsetool.query import describe_collection, get_supported_params, query_features


def _format_size(size_bytes: int) -> str:
    """Format size in bytes to human readable format."""
    size: float = size_bytes
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


app = typer.Typer(no_args_is_help=True)

query_app = typer.Typer(no_args_is_help=True)
app.add_typer(query_app, name="query")


def _print_attributes(attributes: dict, compact: bool = False) -> None:
    """Print attribute details in a formatted way."""
    for key, attr in attributes.items():
        if compact:
            attr_type = attr.get("type", "")
            print(f"  - {key}" + (f" ({attr_type})" if attr_type else ""))
        else:
            print(f"  - {key}")
            if attr.get("title"):
                print(f"      Description: {attr.get('title')}")
            if attr.get("type"):
                print(f"      Type: {attr.get('type')}")
            if attr.get("example"):
                print(f"      Example: {attr.get('example')}")
            if attr.get("minInclusive"):
                print(f"      Min: {attr.get('minInclusive')}")
            if attr.get("maxInclusive"):
                print(f"      Max: {attr.get('maxInclusive')}")


@query_app.command("search-terms")
def query_search_terms(
    collection: Annotated[
        Optional[str],
        typer.Argument(
            help="Collection name (e.g., SENTINEL-1, SENTINEL-2). "
            "If omitted, shows only supported parameters without querying the server."
        ),
    ] = None,
) -> None:
    """
    List the available search terms for a collection
    """
    if collection is None:
        # No collection specified - show only supported params (no API call)
        supported = get_supported_params()
        print("Supported search terms (use with --search-term):")
        print()
        _print_attributes(supported)
        print()
        print(
            "Specify a collection name to see additional server-available attributes."
        )
    else:
        # Collection specified - fetch from API and show both categories
        result = describe_collection(collection)
        supported = result.get("supported", {})
        available = result.get("available", {})

        print(f"Search terms for collection {collection}:")
        print()
        print("SUPPORTED (can be used in --search-term):")
        if supported:
            _print_attributes(supported)
        else:
            print("  (none)")

        print()
        print("AVAILABLE ON SERVER (not yet supported by CDSETool):")
        if available:
            _print_attributes(available, compact=True)
        else:
            print("  (none)")


# TODO: implement limit
@query_app.command("search")
def query_search(
    collection: str,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs (e.g., startDate=2024-01-01). "
            + "Pass multiple times for multiple search terms"
        ),
    ] = None,
    json: Annotated[bool, typer.Option(help="Output JSON")] = False,
    count_only: Annotated[
        bool,
        typer.Option(
            "--count-only",
            help="Only show query URL and total result count "
            "without fetching all results",
        ),
    ] = False,
) -> None:
    """
    Search for features matching the search terms
    """
    search_term = search_term or []
    search_terms = _to_dict(search_term)

    if count_only:
        # Fetch no products, only the count
        search_terms["top"] = 0
        products = query_features(collection, search_terms)
        print(f"URL: {products.get_url()}")
        print(f"Total results: {len(products)}")
        return

    features = query_features(collection, search_terms)

    for feature in features:
        if json:
            print(JSON.dumps(feature))
        else:
            print(feature.get("Name"))


# TODO: implement limit
@app.command("download")
def download(  # pylint: disable=[too-many-arguments, too-many-positional-arguments]
    collection: str,
    path: Annotated[
        Optional[str],
        typer.Argument(help="Output directory path (required unless --list-only)"),
    ] = None,
    concurrency: Annotated[
        int, typer.Option(help="Number of concurrent connections")
    ] = 1,
    overwrite_existing: Annotated[
        bool, typer.Option(help="Overwrite already downloaded files")
    ] = False,
    search_term: Annotated[
        Optional[List[str]],
        typer.Option(
            help="Search by term=value pairs (e.g., startDate=2024-01-01). "
            + "Pass multiple times for multiple search terms"
        ),
    ] = None,
    filter_pattern: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Download specific files within product bundles using OData API's node"
                " filtering functionality"
            )
        ),
    ] = None,
    list_only: Annotated[
        bool,
        typer.Option(
            "--list-only",
            help="List products and download URLs without downloading "
            "(no credentials required)",
        ),
    ] = False,
) -> None:
    """
    Download all features matching the search terms
    """
    search_term = search_term or []
    features = query_features(collection, _to_dict(search_term))

    if list_only:
        count = 0
        total_size = 0
        for product in features:
            info = get_product_download_info(product)
            print(f"{info['name']}")
            print(f"  Date: {info['date']}")
            print(f"  Size: {_format_size(info['size'])}")
            print(f"  URL:  {info['url']}")
            count += 1
            total_size += info["size"]
        print()
        print(f"Total: {count} products, {_format_size(total_size)}")
        return

    if not path:
        print("Error: PATH is required unless --list-only is specified")
        sys.exit(1)

    if not os.path.exists(path):
        print(f"Path {path} does not exist")
        sys.exit(1)

    list(
        download_features(
            features,
            path,
            {
                "monitor": StatusMonitor(),
                "concurrency": concurrency,
                "overwrite_existing": overwrite_existing,
                "filter_pattern": filter_pattern,
            },
        )
    )


def main():
    """
    Main entry point
    """
    app()


def _to_dict(term_list: List[str]) -> Dict[str, Any]:
    search_terms = {}
    for item in term_list:
        key, value = item.split("=")
        search_terms[key] = value
    return search_terms


if __name__ == "__main__":
    main()
