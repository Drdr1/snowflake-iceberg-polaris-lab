from __future__ import annotations

import os
import uuid

import pytest

from lab.catalog import NAMESPACE, catalog_kind, drop_table_if_exists, ensure_namespace, get_catalog
from lab.data import ORDERS_SCHEMA


@pytest.fixture(scope="session")
def catalog(tmp_path_factory):
    if catalog_kind() == "sql":
        cat = get_catalog(tmp_path_factory.mktemp("warehouse"))
    else:
        cat = get_catalog()
    ensure_namespace(cat)
    return cat


@pytest.fixture
def table_id(catalog, request):
    """A unique table name per test, dropped afterwards."""
    name = f"{NAMESPACE}.{request.node.name[:40]}_{uuid.uuid4().hex[:6]}".lower()
    name = "".join(c if c.isalnum() or c in "._" else "_" for c in name)
    yield name
    if not os.environ.get("LAB_KEEP_TABLES"):
        drop_table_if_exists(catalog, name)


@pytest.fixture
def orders_table(catalog, table_id):
    return catalog.create_table(table_id, schema=ORDERS_SCHEMA)


def pytest_report_header(config):
    return f"lab catalog: {catalog_kind()}"
