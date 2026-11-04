"""Compare a table's fingerprint across engines.

    # Snowflake numbers from snowflake/50_checks.sql (copy the result row)
    python -m lab.compare lab.orders --snowflake "10400,1,10400,5213387.21"

    # Or compare against a metadata file directly (Snowflake-managed tables)
    python -m lab.compare --metadata s3://bucket/.../00003-x.metadata.json --snowflake "..."

Exit code 0 = identical, 1 = mismatch. Prints both sides as JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal

from lab.checks import Fingerprint, fingerprint


def parse_snowflake(raw: str) -> Fingerprint:
    parts = [x.strip() for x in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("expected 4 values: row_count,min_order_id,max_order_id,amount_sum")
    n, lo, hi, total = parts
    none = {"", "null", "none", "NULL"}
    return Fingerprint(
        row_count=int(n),
        min_order_id=None if lo in none else int(lo),
        max_order_id=None if hi in none else int(hi),
        amount_sum=None if total in none else Decimal(total).quantize(Decimal("0.01")),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("table", nargs="?", help="namespace.table in the configured catalog")
    src.add_argument("--metadata", help="metadata.json URI to read with PyIceberg (no catalog)")
    p.add_argument("--snowflake", required=True, help="row_count,min,max,sum from 50_checks.sql")
    args = p.parse_args(argv)

    if args.metadata:
        from lab.read_metadata import load_static

        table = load_static(args.metadata)
    else:
        from lab.catalog import get_catalog

        table = get_catalog().load_table(args.table)

    iceberg_side = fingerprint(table)
    snowflake_side = parse_snowflake(args.snowflake)
    same = iceberg_side == snowflake_side
    print(json.dumps({"iceberg": iceberg_side.as_dict(), "snowflake": snowflake_side.as_dict(), "match": same}, indent=2))
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
