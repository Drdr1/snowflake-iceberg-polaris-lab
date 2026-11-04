"""The metadata checker must catch real breakage, not just pass on happy paths."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import pytest

from lab.catalog import catalog_kind
from lab.checks import Fingerprint, fingerprint, fingerprint_arrow, metadata_consistency
from lab.data import orders


def test_fresh_table_is_consistent(orders_table):
    orders_table.append(orders(100))
    report = metadata_consistency(orders_table)
    assert report.ok, report.problems


@pytest.mark.skipif(catalog_kind() != "sql", reason="deletes a file on local disk")
def test_detects_missing_data_file(orders_table):
    t = orders_table
    t.append(orders(10))
    t.append(orders(10, start_id=11))
    victim = t.inspect.files().column("file_path")[0].as_py()
    os.remove(urlparse(victim).path)

    report = metadata_consistency(t)
    assert not report.ok
    assert any("missing data file" in p for p in report.problems)


def test_fingerprint_matches_direct_arrow(orders_table):
    batch = orders(77)
    orders_table.append(batch)
    assert fingerprint(orders_table) == fingerprint_arrow(batch)


def test_empty_table_fingerprint(orders_table):
    assert fingerprint(orders_table) == Fingerprint(0, None, None, None)
