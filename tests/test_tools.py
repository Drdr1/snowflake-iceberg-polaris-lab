"""The small CLI helpers used in the Snowflake runbook."""

from __future__ import annotations

from decimal import Decimal

import pytest

from lab.checks import Fingerprint, fingerprint
from lab.compare import parse_snowflake
from lab.data import orders
from lab.locate import relative_to_bucket
from lab.read_metadata import load_static


def test_parse_snowflake_row():
    fp = parse_snowflake("500, 1, 500, 249999.5")
    assert fp == Fingerprint(500, 1, 500, Decimal("249999.50"))


def test_parse_snowflake_empty_table():
    assert parse_snowflake("0,NULL,NULL,NULL") == Fingerprint(0, None, None, None)


def test_parse_snowflake_rejects_wrong_shape():
    with pytest.raises(ValueError):
        parse_snowflake("1,2,3")


def test_relative_to_bucket():
    uri = "s3://lakehouse/lab_catalog/lab/orders/metadata/00002-a.metadata.json"
    assert relative_to_bucket(uri) == "lab_catalog/lab/orders/metadata/00002-a.metadata.json"


def test_static_read_matches_catalog_read(orders_table):
    orders_table.append(orders(60))
    static = load_static(orders_table.metadata_location)
    assert fingerprint(static) == fingerprint(orders_table)
