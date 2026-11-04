"""Spark <-> PyIceberg through the same Polaris catalog.

This is the local version of the Snowflake <-> Spark question: two different
engines, one REST catalog, one copy of the data. Each test writes with one
engine and checks with the other.

Runs only with LAB_CATALOG=rest and LAB_SPARK=1 (the CI "stack" job).
"""

from __future__ import annotations

import datetime as _dt
import os
import uuid

import pytest

from lab.catalog import NAMESPACE, catalog_kind
from lab.checks import fingerprint, metadata_consistency
from lab.data import orders


def _utc_now() -> str:
    # Iceberg procedure arguments must be literals, not current_timestamp().
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(
        catalog_kind() != "rest" or not os.environ.get("LAB_SPARK"),
        reason="needs Polaris (LAB_CATALOG=rest) and Spark (LAB_SPARK=1)",
    ),
]


@pytest.fixture(scope="module")
def spark():
    from spark.session import build_session

    s = build_session()
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{NAMESPACE}")
    yield s
    s.stop()


@pytest.fixture
def ident():
    name = f"{NAMESPACE}.spark_{uuid.uuid4().hex[:8]}"
    yield name


def _fp(catalog, ident):
    return fingerprint(catalog.load_table(ident)).as_dict()


def test_spark_writes_pyiceberg_reads(spark, catalog, ident):
    from spark.session import SPARK_ORDERS_DDL, arrow_to_spark, spark_fingerprint

    spark.sql(f"CREATE TABLE polaris.{ident} ({SPARK_ORDERS_DDL}) USING iceberg PARTITIONED BY (months(order_date))")
    arrow_to_spark(spark, orders(500)).writeTo(f"polaris.{ident}").append()

    assert _fp(catalog, ident) == spark_fingerprint(spark, ident)
    t = catalog.load_table(ident)
    assert [f.transform.__class__.__name__ for f in t.spec().fields] == ["MonthTransform"]
    assert metadata_consistency(t).ok


def test_pyiceberg_writes_spark_reads(spark, catalog, ident):
    from lab.data import ORDERS_SCHEMA
    from spark.session import spark_fingerprint

    t = catalog.create_table(ident, schema=ORDERS_SCHEMA)
    t.append(orders(300))
    assert spark_fingerprint(spark, ident) == _fp(catalog, ident)


def test_spark_schema_evolution_visible_to_pyiceberg(spark, catalog, ident):
    from spark.session import SPARK_ORDERS_DDL, arrow_to_spark

    spark.sql(f"CREATE TABLE polaris.{ident} ({SPARK_ORDERS_DDL}) USING iceberg")
    arrow_to_spark(spark, orders(100)).writeTo(f"polaris.{ident}").append()
    status_id = catalog.load_table(ident).schema().find_field("status").field_id

    spark.sql(f"ALTER TABLE polaris.{ident} ADD COLUMN channel STRING")
    spark.sql(f"ALTER TABLE polaris.{ident} RENAME COLUMN status TO order_status")
    spark.sql(f"ALTER TABLE polaris.{ident} ALTER COLUMN customer_id TYPE BIGINT")

    t = catalog.load_table(ident)
    names = [f.name for f in t.schema().fields]
    assert "channel" in names and "order_status" in names and "status" not in names
    assert t.schema().find_field("order_status").field_id == status_id
    data = t.scan(selected_fields=("order_status", "channel")).to_arrow()
    assert data["order_status"].null_count == 0
    assert data["channel"].null_count == 100


def test_spark_partition_evolution_visible_to_pyiceberg(spark, catalog, ident):
    from spark.session import SPARK_ORDERS_DDL, arrow_to_spark, spark_fingerprint

    spark.sql(f"CREATE TABLE polaris.{ident} ({SPARK_ORDERS_DDL}) USING iceberg PARTITIONED BY (months(order_date))")
    arrow_to_spark(spark, orders(200)).writeTo(f"polaris.{ident}").append()
    spark.sql(f"ALTER TABLE polaris.{ident} REPLACE PARTITION FIELD months(order_date) WITH days(order_date)")
    arrow_to_spark(spark, orders(200, start_id=201)).writeTo(f"polaris.{ident}").append()

    t = catalog.load_table(ident)
    spec_ids = set(t.inspect.files().column("spec_id").to_pylist())
    assert len(spec_ids) == 2
    assert _fp(catalog, ident) == spark_fingerprint(spark, ident)


def test_spark_compaction_and_expiry(spark, catalog, ident):
    from spark.session import SPARK_ORDERS_DDL, arrow_to_spark

    spark.sql(f"CREATE TABLE polaris.{ident} ({SPARK_ORDERS_DDL}) USING iceberg")
    for i in range(8):
        arrow_to_spark(spark, orders(25, start_id=1 + i * 25)).coalesce(1).writeTo(f"polaris.{ident}").append()

    before = catalog.load_table(ident)
    files_before = before.inspect.files().num_rows
    fp_before = fingerprint(before)
    assert files_before >= 8

    spark.sql(f"CALL polaris.system.rewrite_data_files(table => '{ident}', options => map('min-input-files', '2'))")
    after = catalog.load_table(ident)
    assert after.inspect.files().num_rows < files_before
    assert fingerprint(after) == fp_before
    assert metadata_consistency(after).ok

    snaps_before = len(after.snapshots())
    spark.sql(
        f"CALL polaris.system.expire_snapshots(table => '{ident}', "
        f"older_than => TIMESTAMP '{_utc_now()}', retain_last => 1)"
    )
    final = catalog.load_table(ident)
    assert len(final.snapshots()) < snaps_before
    assert fingerprint(final) == fp_before


def test_concurrent_appends_from_both_engines(spark, catalog, ident):
    """Optimistic concurrency: a stale writer is rejected, never silently wins.

    The catalog checks the writer's base snapshot on commit. A writer holding
    old metadata either rebases or gets a CommitFailedException and must
    refresh. Either way, no rows are lost or duplicated.
    """
    from pyiceberg.exceptions import CommitFailedException

    from spark.session import SPARK_ORDERS_DDL, arrow_to_spark

    spark.sql(f"CREATE TABLE polaris.{ident} ({SPARK_ORDERS_DDL}) USING iceberg")
    stale = catalog.load_table(ident)  # PyIceberg holds the old metadata
    arrow_to_spark(spark, orders(50)).writeTo(f"polaris.{ident}").append()
    try:
        stale.append(orders(50, start_id=51))
    except CommitFailedException:
        catalog.load_table(ident).append(orders(50, start_id=51))

    fp = _fp(catalog, ident)
    assert fp["row_count"] == 100
    assert (fp["min_order_id"], fp["max_order_id"]) == (1, 100)
