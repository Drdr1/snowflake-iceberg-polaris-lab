"""Table maintenance: snapshots, time travel, expiry, small-file compaction.

Real compaction (rewrite_data_files) is a Spark procedure and is tested in
tests/test_spark_interop.py. Here we cover what every engine relies on:
snapshot expiry semantics and that rewriting files never changes the data.
"""

from __future__ import annotations

import datetime as dt
import time

import pytest

from lab.checks import fingerprint, metadata_consistency
from lab.data import orders


def data_file_count(table) -> int:
    return table.inspect.files().num_rows


def test_time_travel_reads_old_snapshot(orders_table):
    t = orders_table
    t.append(orders(20))
    first = t.current_snapshot().snapshot_id
    fp_first = fingerprint(t)
    t.append(orders(20, start_id=21))

    assert fingerprint(t).row_count == 40
    assert fingerprint(t, snapshot_id=first) == fp_first


def test_small_files_then_rewrite_keeps_data(orders_table):
    t = orders_table
    for i in range(8):  # 8 tiny commits -> 8 tiny files: the streaming-ingest problem
        t.append(orders(10, start_id=1 + i * 10))
    assert data_file_count(t) == 8
    before = fingerprint(t)

    # Logical rewrite into one file. Spark's rewrite_data_files does this
    # per partition with size targets; the invariant is the same.
    t.overwrite(t.scan().to_arrow())
    t = t.refresh()

    assert data_file_count(t) == 1
    assert fingerprint(t) == before
    assert metadata_consistency(t).ok


def test_expire_snapshots_keeps_current_and_tagged(orders_table):
    t = orders_table
    for i in range(4):
        t.append(orders(5, start_id=1 + i * 5))
    snaps = [s.snapshot_id for s in t.snapshots()]
    tagged = snaps[1]
    t.manage_snapshots().create_tag(tagged, "month_end_close").commit()
    t = t.refresh()
    current = t.current_snapshot().snapshot_id
    expected_tagged_fp = fingerprint(t, snapshot_id=tagged)

    time.sleep(0.01)
    t.maintenance.expire_snapshots().older_than(dt.datetime.now(dt.timezone.utc)).commit()
    t = t.refresh()

    kept = {s.snapshot_id for s in t.snapshots()}
    assert current in kept
    assert tagged in kept  # refs protect snapshots from expiry
    assert snaps[0] not in kept
    assert fingerprint(t).row_count == 20
    assert fingerprint(t, snapshot_id=tagged) == expected_tagged_fp


def test_expired_snapshot_cannot_be_read(orders_table):
    t = orders_table
    t.append(orders(5))
    old = t.current_snapshot().snapshot_id
    t.append(orders(5, start_id=6))
    t = t.refresh()
    t.maintenance.expire_snapshots().by_id(old).commit()
    t = t.refresh()

    assert t.snapshot_by_id(old) is None
    with pytest.raises(ValueError):
        t.scan(snapshot_id=old).to_arrow()
