"""Print a table's current metadata file, for Snowflake OBJECT_STORE tables.

    python -m lab.locate lab.orders            # path relative to the bucket
    python -m lab.locate lab.orders --full     # full s3:// URI
"""

from __future__ import annotations

import argparse
from urllib.parse import urlparse

from lab.catalog import get_catalog


def relative_to_bucket(uri: str) -> str:
    return urlparse(uri).path.lstrip("/")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("table", help="namespace.table, e.g. lab.orders")
    p.add_argument("--full", action="store_true", help="print the full URI")
    args = p.parse_args(argv)

    t = get_catalog().load_table(args.table)
    print(t.metadata_location if args.full else relative_to_bucket(t.metadata_location))


if __name__ == "__main__":
    main()
