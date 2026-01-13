"""
Command line interface
"""

import json as JSON
import os
import sys
from typing import Any, Dict, List, Optional

import typer
from typing_extensions import Annotated

from cdsetool.download import download_features
from cdsetool.monitor import StatusMonitor
from cdsetool.query import describe_collection, query_features

app = typer.Typer(no_args_is_help=True)

query_app = typer.Typer(no_args_is_help=True)
app.add_typer(query_app, name="query")


@query_app.command("search-terms")
def query_search_terms(collection: str) -> None:
    """
    List the available search terms for a collection
    """
    print(f"Available search terms for collection {collection}:")
    # TODO: print validators
    for key, attributes in describe_collection(collection).items():
        print(f"  - {key}")
        if attributes.get("title"):
            print(f"    - Description: {attributes.get('title')}")
        if attributes.get("example"):
            print(f"      Example: {attributes.get('example')}")
        if attributes.get("minInclusive"):
            print(f"    - Min: {attributes.get('minInclusive')}")
        if attributes.get("maxInclusive"):
            print(f"    - Max: {attributes.get('maxInclusive')}")

        print()


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
    path: str,
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
) -> None:
    """
    Download all features matching the search terms
    """
    if not os.path.exists(path):
        print(f"Path {path} does not exist")
        sys.exit(1)

    search_term = search_term or []
    features = query_features(collection, _to_dict(search_term))

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
