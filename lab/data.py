"""Deterministic synthetic order data. No real customers, ever."""

from __future__ import annotations

import datetime as dt
import random

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

REGIONS = ["us-east", "us-west", "eu-central", "ap-south"]
STATUSES = ["placed", "paid", "shipped", "returned"]

# Iceberg schema v1. Field IDs are explicit so tests can assert that schema
# evolution keeps IDs stable (that is what makes rename safe across engines).
ORDERS_SCHEMA = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "customer_id", IntegerType(), required=False),
    NestedField(3, "region", StringType(), required=False),
    NestedField(4, "status", StringType(), required=False),
    NestedField(5, "amount", DecimalType(10, 2), required=False),
    NestedField(6, "order_date", DateType(), required=False),
    NestedField(7, "updated_at", TimestamptzType(), required=False),
    identifier_field_ids=[1],
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int32()),
        pa.field("region", pa.string()),
        pa.field("status", pa.string()),
        pa.field("amount", pa.decimal128(10, 2)),
        pa.field("order_date", pa.date32()),
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


def orders(n: int, start_id: int = 1, seed: int = 7, day0: dt.date = dt.date(2026, 1, 1)) -> pa.Table:
    """Return ``n`` orders with ids ``start_id..start_id+n-1``.

    Dates spread over 60 days so day/month partitioning produces several
    partitions. Same arguments always give the same rows.
    """
    from decimal import Decimal

    rng = random.Random(seed + start_id)
    rows = {name: [] for name in ARROW_SCHEMA.names}
    for i in range(n):
        d = day0 + dt.timedelta(days=rng.randrange(60))
        rows["order_id"].append(start_id + i)
        rows["customer_id"].append(rng.randrange(1, 500))
        rows["region"].append(rng.choice(REGIONS))
        rows["status"].append(rng.choice(STATUSES))
        rows["amount"].append(Decimal(rng.randrange(100, 100_000)) / 100)
        rows["order_date"].append(d)
        rows["updated_at"].append(
            dt.datetime.combine(d, dt.time(rng.randrange(24), rng.randrange(60)), tzinfo=dt.timezone.utc)
        )
    return pa.Table.from_pydict(rows, schema=ARROW_SCHEMA)
