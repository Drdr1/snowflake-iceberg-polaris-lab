# snowflake-iceberg-polaris-lab

A hands-on lab for **Snowflake + Apache Iceberg + Apache Polaris + Spark on S3**:
one copy of the data in object storage, one open REST catalog, and several
engines reading and writing the same tables.

It answers the practical questions an open-lakehouse POC has to answer:

- Does a table Spark writes read the same in Snowflake, and the reverse?
- What happens to schema evolution, partition evolution, compaction and
  snapshot expiry when two engines share a table?
- Which Snowflake catalog integration fits which situation: object store,
  Iceberg REST (Polaris) with vended credentials, catalog-linked database,
  or Snowflake-managed tables?
- How do IAM, roles and read-only access fit together across AWS, Polaris and Snowflake?

[![ci](https://github.com/Drdr1/snowflake-iceberg-polaris-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/Drdr1/snowflake-iceberg-polaris-lab/actions/workflows/ci.yml)

## What is verified, and where

| Layer | How it runs | Status |
|---|---|---|
| Iceberg behaviour (schema, partitions, snapshots, consistency) | PyIceberg tests on a local SQL catalog | CI, every push |
| Same tests against **Polaris REST + MinIO (S3 API)** | `docker compose` in GitHub Actions | CI, every push |
| **Spark ⇄ PyIceberg** interop, compaction, expiry, concurrent writers | Spark 3.5 + Iceberg 1.10.1 in CI | CI, every push |
| Polaris RBAC: Snowflake's principal is read-only | `polaris/check_reader.sh` in CI | CI, every push |
| **Snowflake** external volume, REST catalog integration, catalog-linked DB, managed Iceberg | SQL in `snowflake/`, run against a trial account | Written; live results in [docs/FINDINGS.md](docs/FINDINGS.md) |

The Snowflake side needs a real account and a public Polaris endpoint, so it
is not part of CI. [docs/FINDINGS.md](docs/FINDINGS.md) only marks what has
actually been run.

## Architecture

```text
                        +---------------------------+
   Spark 3.5 ---------->|  Apache Polaris           |<--------- Snowflake
   (writes, evolves,    |  Iceberg REST catalog     |  CATALOG INTEGRATION
    compacts)           |  RBAC, credential vending |  (ICEBERG_REST, read-only principal)
   PyIceberg ---------->|                           |
   (tests, checks)      +-------------+-------------+
                                      | table-scoped temporary credentials
                                      v
                         S3 (MinIO locally, AWS S3 live)
                         s3://bucket/lab_catalog/lab/orders/{data,metadata}
                                      ^
             Snowflake-managed Iceberg tables (CATALOG='SNOWFLAKE')
             written via EXTERNAL VOLUME, read back by PyIceberg
```

## Repository layout

```text
lab/            catalog loading, synthetic data, fingerprint + metadata checks, CLI helpers
tests/          schema / partition / maintenance / consistency / Spark interop tests
spark/          Spark session for Polaris, demo loader (create -> evolve -> maintain), benchmark SQL
polaris/        bootstrap (catalog, grants, read-only Snowflake principal), RBAC check
snowflake/      00 roles -> 10 external volume -> 20/21/22 catalog integrations -> 30 managed -> 50 checks -> 60 cost
aws/            IAM policy and trust templates (Snowflake role, Polaris role, Polaris server user)
deploy/vps/     Polaris on a VPS behind TLS, backed by real S3
docs/           RUNBOOK.md (step by step) and FINDINGS.md (results)
```

## Quick start

```bash
pip install -r requirements.txt
python -m pytest -v                      # local, no Docker, no accounts

docker compose up -d --wait              # MinIO + Polaris
bash polaris/bootstrap.sh
LAB_CATALOG=rest python -m pytest -v -m "not spark"
```

Spark interop needs Java 17: `pip install -r requirements-spark.txt`, then
`LAB_CATALOG=rest LAB_SPARK=1 python -m pytest -v -m spark`.

For the live Snowflake + S3 path, follow [docs/RUNBOOK.md](docs/RUNBOOK.md).

## Design choices

- **Fingerprints, not eyeballing.** Row count, min/max key and the sum of a
  decimal column are computed the same way in PyIceberg, Spark SQL and
  Snowflake SQL. `python -m lab.compare` diffs them. Decimal sums catch
  precision drift that row counts miss.
- **Field IDs in every schema test.** Iceberg interoperability depends on
  stable field IDs, so the tests check IDs rather than column names.
- **Least privilege by default.** Snowflake connects to Polaris with a
  read-only principal, and the linked database is `ALLOWED_WRITE_OPERATIONS = NONE`.
  The Polaris server's AWS user may only assume one role; engines get
  short-lived, table-scoped credentials.
- **Synthetic data only.** `lab/data.py` is deterministic and fake.

## Versions

Apache Iceberg 1.10.1 (Spark runtime) · Spark 3.5.9 · PyIceberg 0.12.0 ·
Apache Polaris (Docker image `apache/polaris`, pin with `POLARIS_VERSION`) ·
MinIO RELEASE.2025-04-22 (Docker Hub).

MinIO is only a local stand-in for S3. The live path uses AWS S3.
