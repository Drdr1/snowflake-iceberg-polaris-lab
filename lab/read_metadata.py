"""Read an Iceberg table straight from its metadata.json, no catalog.

This is how a non-Snowflake engine reads a Snowflake-managed table: take
metadataLocation from SYSTEM$GET_ICEBERG_TABLE_INFORMATION and open it.

    python -m lab.read_metadata s3://bucket/snowflake_managed/orders_sf/metadata/x.metadata.json

S3 credentials come from the normal AWS environment (AWS_PROFILE or keys).
"""

from __future__ import annotations

import argparse
import json
import os

from pyiceberg.table import StaticTable

from lab.checks import fingerprint, metadata_consistency


def load_static(metadata_uri: str) -> StaticTable:
    props = {}
    if os.environ.get("AWS_REGION"):
        props["s3.region"] = os.environ["AWS_REGION"]
    if os.environ.get("S3_ENDPOINT"):  # MinIO
        props["s3.endpoint"] = os.environ["S3_ENDPOINT"]
    return StaticTable.from_metadata(metadata_uri, properties=props)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("metadata_uri")
    args = p.parse_args(argv)

    t = load_static(args.metadata_uri)
    report = metadata_consistency(t)
    out = {
        "schema": [f"{f.field_id}:{f.name}:{f.field_type}" for f in t.schema().fields],
        "partition_spec": str(t.spec()),
        "snapshots": len(t.snapshots()),
        "fingerprint": fingerprint(t).as_dict() if "order_id" in t.schema().column_names else None,
        "metadata_problems": report.problems,
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
