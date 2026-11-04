# Findings

What has actually been run, where, and what it showed. Nothing goes in the
"Verified" column until it has really been run there.

## Status

| Area | Local SQL catalog | Polaris + RustFS + Spark (CI) | Live Snowflake trial + S3 |
|---|---|---|---|
| Schema evolution (add / rename / widen / drop + re-add / reorder) | ✅ tests | ✅ tests (also via Spark DDL) | ⏳ pending |
| Partition evolution (month → day, identity + bucket) | ✅ tests | ✅ tests (Spark `REPLACE PARTITION FIELD`) | ⏳ pending |
| Compaction (`rewrite_data_files`) | logical rewrite only | ✅ tests | ⏳ pending |
| Snapshot expiry; tags protect snapshots | ✅ tests | ✅ tests (Spark procedure) | ⏳ pending |
| Metadata consistency checker catches missing files | ✅ tests | ✅ tests | — |
| Spark ⇄ PyIceberg through one REST catalog | — | ✅ tests | — |
| Concurrent writers, stale metadata | — | ✅ tests | — |
| Read-only principal enforced by Polaris RBAC | — | ✅ `check_reader.sh` (list/load 200, create/drop 403) | ⏳ pending |
| Snowflake external volume + IAM handshake | — | — | ⏳ pending |
| Snowflake OBJECT_STORE integration | — | — | ⏳ pending |
| Snowflake ICEBERG_REST → Polaris, vended credentials | — | — | ⏳ pending |
| Catalog-linked database discovery | — | — | ⏳ pending |
| Snowflake-managed Iceberg read by PyIceberg | — | — | ⏳ pending |
| Snowflake vs Spark cost/performance method | — | — | ⏳ pending |

✅ = passes in this repo's tests · ⏳ = scripts written, not yet run against a live account

## Behaviour confirmed by the tests

- **Rename is safe across engines** because columns are matched by field ID.
  Old Parquet files still contain `status`; readers return it as `order_status`.
- **A dropped and re-added column does not get its old data back.** It gets a
  new field ID, so old files read as NULL. An engine that matched columns by
  name would show the old values again.
- **Narrowing a type is rejected** (long → int). Only safe promotions are allowed.
- **Partition evolution affects new files only.** Old files keep their spec ID,
  and filters on `order_date` still prune across both specs.
- **Tags protect snapshots from expiry.** A tagged snapshot (e.g. month-end
  close) survives `expire_snapshots`. Expired, untagged snapshots can no
  longer be read.
- **A stale writer cannot overwrite a newer commit.** The catalog validates
  the writer's base snapshot, so no rows are lost when two engines write.
- **Polaris `*_FULL_METADATA` privileges are not read-only.** The first
  version of the Snowflake "reader" role had `NAMESPACE_FULL_METADATA` and
  `TABLE_FULL_METADATA`. CI showed that principal could create a namespace and
  attempt drops. The fix grants only `CATALOG_READ_PROPERTIES`,
  `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`,
  `TABLE_READ_PROPERTIES` and `TABLE_READ_DATA`. `polaris/check_reader.sh`
  now proves list/load succeed and create/drop return 403.
- **Compaction rewrites into the current partition spec, which can add files.**
  In the demo, months → days evolution then `rewrite_data_files` moved the old
  month-partitioned data into daily partitions: 62 data files became 119 for
  10,400 rows, with the same fingerprint. Compaction cannot fix a spec that is
  too fine for the data volume; choose the partition grain from row counts first.
- **A catalog-less read needs its own storage credentials.** Reading a table
  straight from its metadata.json (how outside engines read Snowflake-managed
  tables) bypasses Polaris, so there is no credential vending: the first CI
  run failed with S3 `ACCESS_DENIED` until the reader got its own keys. Plan
  IAM for that read path separately.
- **The consistency checker** cross-checks the snapshot summary, manifest
  record counts and an actual scan, and detects data files missing from
  storage before a scan fails on them.

## Live Snowflake results

_To be filled in while following [RUNBOOK.md](RUNBOOK.md). For each item:
the command, what happened, the exact error text if any, and the fix._

- Snowflake account region / bucket region:
- External volume verify: 
- REST integration verify: 
- Auto-refresh lag after a Spark commit (seconds): 
- Schema after `evolve` as Snowflake shows it: 
- Pruning across partition specs (partitions scanned / total): 
- Time travel to a Spark snapshot id: 
- Snowflake-managed table read by PyIceberg (types, field IDs): 
- Benchmark (Snowflake vs Spark: time, bytes, credits): 
