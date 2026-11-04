"""Catalog loading for the lab.

The same tests run against two catalogs:

* ``sql``  - a local SQLite-backed Iceberg catalog with a file warehouse.
             Needs nothing but Python. This is what runs on a laptop.
* ``rest`` - Apache Polaris (Iceberg REST catalog) with MinIO as S3.
             This is what runs in CI after ``docker compose up``.

Pick one with ``LAB_CATALOG=sql|rest``. Defaults to ``sql``.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

NAMESPACE = os.environ.get("LAB_NAMESPACE", "lab")


def catalog_kind() -> str:
    kind = os.environ.get("LAB_CATALOG", "sql").lower()
    if kind not in {"sql", "rest"}:
        raise ValueError(f"LAB_CATALOG must be 'sql' or 'rest', got {kind!r}")
    return kind


def rest_properties() -> dict[str, str]:
    """Properties for Polaris. Mirrors the Spark config in spark/session.py.

    S3_ENDPOINT defaults to local MinIO. Set it to an empty string for real
    AWS S3, where the endpoint and credentials come from Polaris.
    """
    props = {
        "type": "rest",
        "uri": os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"),
        "warehouse": os.environ.get("POLARIS_CATALOG", "lab_catalog"),
        "credential": os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"),
        "scope": "PRINCIPAL_ROLE:ALL",
        # Ask Polaris to vend short-lived storage credentials, the same way
        # Snowflake does with ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS.
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "s3.region": os.environ.get("AWS_REGION", "us-west-2"),
    }
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    if endpoint:
        props["s3.endpoint"] = endpoint
        props["s3.path-style-access"] = "true"
    return props


def sql_properties(warehouse: Path) -> dict[str, str]:
    warehouse.mkdir(parents=True, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{warehouse / 'catalog.db'}",
        "warehouse": f"file://{warehouse}",
    }


def get_catalog(warehouse: Path | None = None) -> Catalog:
    kind = catalog_kind()
    if kind == "rest":
        return load_catalog("polaris", **rest_properties())
    if warehouse is None:
        warehouse = Path(os.environ.get("LAB_WAREHOUSE", ".warehouse")).resolve()
    return load_catalog("local", **sql_properties(warehouse))


def ensure_namespace(catalog: Catalog, namespace: str = NAMESPACE) -> str:
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)
    return namespace


def drop_table_if_exists(catalog: Catalog, identifier: str) -> None:
    # Plain drop (no purge): Polaris needs an extra privilege and feature flag
    # to purge files, and tests use unique table names per run anyway.
    if catalog.table_exists(identifier):
        catalog.drop_table(identifier)
