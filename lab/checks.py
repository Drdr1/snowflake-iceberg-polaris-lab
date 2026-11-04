"""Engine-independent checks used by the tests and by the Snowflake comparison.

``fingerprint`` is the core interop check: the same four numbers are computed
here from Iceberg data and in ``snowflake/50_checks.sql`` from Snowflake. If a
table written by one engine reads differently in another, the fingerprints
disagree.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.table import Table


@dataclass(frozen=True)
class Fingerprint:
    row_count: int
    min_order_id: int | None
    max_order_id: int | None
    amount_sum: Decimal | None

    def as_dict(self) -> dict:
        d = asdict(self)
        d["amount_sum"] = None if self.amount_sum is None else str(self.amount_sum)
        return d


def fingerprint_arrow(t: pa.Table) -> Fingerprint:
    if t.num_rows == 0:
        return Fingerprint(0, None, None, None)
    ids = t.column("order_id")
    total = pc.sum(t.column("amount")).as_py()
    return Fingerprint(
        row_count=t.num_rows,
        min_order_id=pc.min(ids).as_py(),
        max_order_id=pc.max(ids).as_py(),
        amount_sum=None if total is None else Decimal(total).quantize(Decimal("0.01")),
    )


def fingerprint(table: Table, snapshot_id: int | None = None) -> Fingerprint:
    scan = table.scan(selected_fields=("order_id", "amount"), snapshot_id=snapshot_id)
    return fingerprint_arrow(scan.to_arrow())


@dataclass
class MetadataReport:
    problems: list[str]

    @property
    def ok(self) -> bool:
        return not self.problems


def metadata_consistency(table: Table, check_files_exist: bool = True) -> MetadataReport:
    """Cross-check what the metadata says against what is actually there."""
    problems: list[str] = []
    meta = table.metadata

    # 1. Schema field IDs are unique and never above last-column-id.
    for schema in meta.schemas:
        ids = [f.field_id for f in schema.fields]
        if len(ids) != len(set(ids)):
            problems.append(f"schema {schema.schema_id}: duplicate field ids {ids}")
        if ids and max(ids) > meta.last_column_id:
            problems.append(f"schema {schema.schema_id}: field id above last-column-id {meta.last_column_id}")

    # 2. Snapshot lineage: every parent we still have is older than its child.
    by_id = {s.snapshot_id: s for s in meta.snapshots}
    for s in meta.snapshots:
        parent = by_id.get(s.parent_snapshot_id) if s.parent_snapshot_id else None
        if parent and parent.timestamp_ms > s.timestamp_ms:
            problems.append(f"snapshot {s.snapshot_id}: parent is newer than child")

    current = table.current_snapshot()
    if current is None:
        return MetadataReport(problems)

    files = table.inspect.files()
    data_files = files.filter(pc.equal(files.column("content"), 0))
    delete_files = files.num_rows - data_files.num_rows

    # 3. Every data file the current snapshot points to actually exists.
    #    Checked first: a scan over a missing file fails instead of reporting.
    if check_files_exist:
        for path in data_files.column("file_path").to_pylist():
            if not table.io.new_input(path).exists():
                problems.append(f"missing data file {path}")
        if problems:
            return MetadataReport(problems)

    # 4. Row count three ways: snapshot summary, manifest file stats, real scan.
    manifest_rows = pc.sum(data_files.column("record_count")).as_py() or 0
    scanned_rows = table.scan(selected_fields=("order_id",)).to_arrow().num_rows

    summary = current.summary.additional_properties if current.summary else {}
    summary_rows = int(summary.get("total-records", -1))
    if delete_files == 0:
        if manifest_rows != scanned_rows:
            problems.append(f"manifest record_count {manifest_rows} != scanned rows {scanned_rows}")
        if summary_rows not in (-1, scanned_rows):
            problems.append(f"snapshot summary total-records {summary_rows} != scanned rows {scanned_rows}")

    return MetadataReport(problems)
