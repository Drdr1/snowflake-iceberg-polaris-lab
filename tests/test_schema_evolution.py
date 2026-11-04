"""Schema evolution: what is safe, what is rejected, and the drop/re-add trap.

Iceberg tracks columns by field ID, not by name or position. Every test here
checks the ID, because that is what decides whether Snowflake and Spark read
old data files correctly after a change.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.exceptions import ValidationError
from pyiceberg.types import IntegerType, LongType, StringType

from lab.checks import fingerprint, metadata_consistency
from lab.data import orders


def field_id(table, name):
    return table.schema().find_field(name).field_id


def test_add_column_old_rows_read_as_null(orders_table):
    t = orders_table
    t.append(orders(50))

    with t.update_schema() as u:
        u.add_column("channel", StringType(), doc="sales channel, added in v2")
    t = t.refresh()

    new_rows = orders(10, start_id=51).append_column("channel", pa.array(["web"] * 10))
    t.append(new_rows)

    data = t.scan().to_arrow()
    assert data.num_rows == 60
    assert field_id(t, "channel") == 8  # next id after the original 7
    old = data.filter(pc.less_equal(data["order_id"], 50))
    assert old["channel"].null_count == 50
    assert metadata_consistency(t).ok


def test_rename_keeps_field_id_and_old_data(orders_table):
    t = orders_table
    t.append(orders(40))
    before = fingerprint(t)
    status_id = field_id(t, "status")

    with t.update_schema() as u:
        u.rename_column("status", "order_status")
    t = t.refresh()

    assert field_id(t, "order_status") == status_id
    data = t.scan(selected_fields=("order_id", "order_status")).to_arrow()
    # Old Parquet files still say "status"; Iceberg maps them by ID.
    assert data["order_status"].null_count == 0
    assert fingerprint(t) == before


def test_widen_int_to_long(orders_table):
    t = orders_table
    t.append(orders(30))
    with t.update_schema() as u:
        u.update_column("customer_id", field_type=LongType())
    t = t.refresh()

    assert isinstance(t.schema().find_field("customer_id").field_type, LongType)
    data = t.scan(selected_fields=("customer_id",)).to_arrow()
    assert data["customer_id"].null_count == 0
    assert pa.types.is_int64(data.schema.field("customer_id").type)


def test_narrowing_is_rejected(orders_table):
    t = orders_table
    with pytest.raises(ValidationError):
        with t.update_schema() as u:
            u.update_column("order_id", field_type=IntegerType())


def test_required_column_cannot_be_added_without_flag(orders_table):
    t = orders_table
    with pytest.raises(ValueError):
        with t.update_schema() as u:
            u.add_column("must_exist", StringType(), required=True)


def test_drop_then_readd_gets_new_id_and_no_old_values(orders_table):
    """The classic trap: re-adding a dropped column does NOT bring data back.

    A name-based engine would show the old values again. Iceberg gives the
    new column a fresh ID, so old files have nothing for it.
    """
    t = orders_table
    t.append(orders(25))
    old_id = field_id(t, "region")

    with t.update_schema() as u:
        u.delete_column("region")
    t = t.refresh()
    with t.update_schema() as u:
        u.add_column("region", StringType())
    t = t.refresh()

    assert field_id(t, "region") != old_id
    data = t.scan(selected_fields=("region",)).to_arrow()
    assert data["region"].null_count == 25
    assert metadata_consistency(t).ok


def test_reorder_does_not_change_ids(orders_table):
    t = orders_table
    ids_before = {f.name: f.field_id for f in t.schema().fields}
    with t.update_schema() as u:
        u.move_first("amount")
    t = t.refresh()
    assert t.schema().fields[0].name == "amount"
    assert {f.name: f.field_id for f in t.schema().fields} == ids_before


def test_schema_history_is_kept(orders_table):
    t = orders_table
    with t.update_schema() as u:
        u.add_column("note", StringType())
    t = t.refresh()
    with t.update_schema() as u:
        u.rename_column("note", "notes")
    t = t.refresh()
    assert len(t.schemas()) == 3
    assert t.metadata.current_schema_id == max(t.schemas())
