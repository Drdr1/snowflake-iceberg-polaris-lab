"""Partitioning and partition evolution.

Iceberg partitioning is hidden: queries filter on order_date, never on a
partition column. Changing the spec only affects new files; old files keep
their spec ID and are still pruned correctly.
"""

from __future__ import annotations

import datetime as dt

import pyarrow.compute as pc

from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform, MonthTransform

from lab.checks import fingerprint, metadata_consistency
from lab.data import ORDERS_SCHEMA, orders


def files_per_spec(table) -> dict[int, int]:
    files = table.inspect.files()
    out: dict[int, int] = {}
    for spec_id in files.column("spec_id").to_pylist():
        out[spec_id] = out.get(spec_id, 0) + 1
    return out


def planned_files(table, row_filter) -> int:
    return sum(1 for _ in table.scan(row_filter=row_filter).plan_files())


def test_month_partitioning_prunes(catalog, table_id):
    t = catalog.create_table(table_id, schema=ORDERS_SCHEMA)
    with t.update_spec() as s:
        s.add_field("order_date", MonthTransform(), "order_month")
    t = t.refresh()
    t.append(orders(300))

    total = sum(files_per_spec(t).values())
    jan = And(GreaterThanOrEqual("order_date", "2026-01-01"), LessThan("order_date", "2026-02-01"))
    assert total >= 2  # data spans Jan-Mar
    assert planned_files(t, jan) < total
    # Pruning never changes the answer.
    jan_rows = t.scan(row_filter=jan).to_arrow().num_rows
    all_rows = t.scan().to_arrow()
    mask = pc.and_(
        pc.greater_equal(all_rows["order_date"], dt.date(2026, 1, 1)),
        pc.less(all_rows["order_date"], dt.date(2026, 2, 1)),
    )
    assert jan_rows == pc.sum(mask.cast("int64")).as_py()


def test_evolve_month_to_day_keeps_old_files(catalog, table_id):
    t = catalog.create_table(table_id, schema=ORDERS_SCHEMA)
    with t.update_spec() as s:
        s.add_field("order_date", MonthTransform(), "order_month")
    t = t.refresh()
    t.append(orders(200))
    old_spec = t.spec().spec_id

    with t.update_spec() as s:
        s.remove_field("order_month")
        s.add_field("order_date", DayTransform(), "order_day")
    t = t.refresh()
    t.append(orders(200, start_id=201))

    specs = files_per_spec(t)
    assert old_spec in specs and t.spec().spec_id in specs
    # spec 0 = unpartitioned (from create), 1 = month, 2 = day
    assert len(t.specs()) == 3
    assert fingerprint(t).row_count == 400

    one_day = EqualTo("order_date", "2026-01-15")
    rows = t.scan(row_filter=one_day).to_arrow()
    assert all(d == dt.date(2026, 1, 15) for d in rows["order_date"].to_pylist())
    assert metadata_consistency(t).ok


def test_add_identity_and_bucket_fields(catalog, table_id):
    t = catalog.create_table(table_id, schema=ORDERS_SCHEMA)
    t.append(orders(100))  # unpartitioned files
    with t.update_spec() as s:
        s.add_field("region", IdentityTransform(), "region")
        s.add_field("customer_id", BucketTransform(8), "customer_bucket")
    t = t.refresh()
    t.append(orders(100, start_id=101))

    specs = files_per_spec(t)
    assert specs[0] == 1          # the original unpartitioned file
    assert sum(specs.values()) > 2  # new data split by region/bucket
    eu = t.scan(row_filter=EqualTo("region", "eu-central")).to_arrow()
    assert set(eu["region"].to_pylist()) == {"eu-central"}
    assert fingerprint(t).row_count == 200
