# Runbook: from laptop to a live Snowflake ⇄ Polaris ⇄ Spark setup

Work top to bottom. Each step says what to record in [FINDINGS.md](FINDINGS.md).
Placeholders: `<BUCKET>`, `<AWS_ACCOUNT_ID>`, `<REGION>`, `<POLARIS_HOST>`.

## 0. Local, no accounts (10 minutes)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest -v                    # Iceberg behaviour on a local SQL catalog
```

With Docker, the full local stack (RustFS as S3, Polaris as REST catalog):

```bash
docker compose up -d --wait
bash polaris/bootstrap.sh              # catalog + read-only principal for Snowflake
LAB_CATALOG=rest python -m pytest -v -m "not spark"
# Spark needs Java 17 and internet (pulls Iceberg jars from Maven):
pip install -r requirements-spark.txt
LAB_CATALOG=rest LAB_SPARK=1 python -m pytest -v -m spark
LAB_CATALOG=rest python -m spark.load_demo --step all
```

GitHub Actions runs all of this on every push (`.github/workflows/ci.yml`).

## 1. Accounts and bucket

1. **Snowflake trial** on **AWS**, in the same region you will use for the
   bucket. Cross-region reads work but add latency and egress cost; note the
   region in FINDINGS.
2. **AWS**: one S3 bucket, versioning off, block public access on:
   ```bash
   aws s3api create-bucket --bucket <BUCKET> --region <REGION> \
     --create-bucket-configuration LocationConstraint=<REGION>   # omit this line for us-east-1
   ```

## 2. Snowflake → S3 (external volume)

1. IAM policy from `aws/s3-access-policy.template.json` (replace `<BUCKET>`),
   attached to a new role `snowflake-iceberg-lab`. For the first trust policy,
   use your own account as principal; it gets replaced in step 4.
2. In Snowflake run `snowflake/00_roles_and_warehouse.sql`, then the CREATE in
   `snowflake/10_external_volume.sql`.
3. `DESC EXTERNAL VOLUME LAB_S3_VOLUME;` → copy `STORAGE_AWS_IAM_USER_ARN`.
4. Save a filled copy of `aws/snowflake-trust-policy.template.json` as
   `aws/snowflake-trust-policy.json` (gitignored) and set it as the
   role's trust policy:
   ```bash
   aws iam update-assume-role-policy --role-name snowflake-iceberg-lab \
     --policy-document file://aws/snowflake-trust-policy.json
   ```
5. `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('LAB_S3_VOLUME');` must report success.

**Record:** any IAM error text you hit and what fixed it. These are the real
troubleshooting notes clients pay for.

## 3. Polaris on the VPS, backed by S3

Snowflake must reach Polaris over public HTTPS, so the local Docker Polaris is
not enough here.

1. IAM: role `polaris-iceberg-lab` with the same S3 policy and
   `aws/polaris-trust-policy.template.json`; IAM user `polaris-lab-server`
   with `aws/polaris-server-user-policy.template.json` and an access key.
   That user can only assume the role; Polaris then vends table-scoped
   credentials to Spark and Snowflake.
2. On the VPS:
   ```bash
   cd deploy/vps && cp polaris.env.example polaris.env   # fill it in
   docker compose up -d --wait
   ```
   Add `Caddyfile.example` to Caddy with your hostname, reload Caddy.
3. Bootstrap against real S3 (from any machine with curl + jq):
   ```bash
   POLARIS_URL=https://<POLARIS_HOST> \
   POLARIS_ROOT_CREDENTIAL='root:<secret>' \
   BASE_LOCATION=s3://<BUCKET>/lab_catalog \
   AWS_REGION=<REGION> S3_ENDPOINT= S3_ENDPOINT_INTERNAL= \
   STORAGE_ROLE_ARN=arn:aws:iam::<AWS_ACCOUNT_ID>:role/polaris-iceberg-lab \
   STORAGE_EXTERNAL_ID=polaris_lab_external_id \
   bash polaris/bootstrap.sh
   ```
   Keep `.polaris-reader.env`: it holds the Snowflake principal's secret.

## 4. Spark writes the demo table to real S3

Easiest without local Java: add repo secrets `POLARIS_URI`
(`https://<POLARIS_HOST>/api/catalog`), `POLARIS_CREDENTIAL` (`root:<secret>`)
and `AWS_REGION`, then run **Actions → aws-demo** with step `create`.

Locally instead:
```bash
export LAB_CATALOG=rest S3_ENDPOINT= AWS_REGION=<REGION> \
  POLARIS_URI=https://<POLARIS_HOST>/api/catalog POLARIS_CREDENTIAL='root:<secret>'
python -m spark.load_demo --step create
```

Note the `snowflake compare string` line it prints.

## 5. Snowflake reads it, three ways

| Script | What it proves | Record |
|---|---|---|
| `20_object_store_catalog.sql` | Reading from a metadata file with no catalog connection. | Whether a new Spark commit shows up without a manual REFRESH (it should not). |
| `21_polaris_rest_catalog.sql` | REST catalog integration with vended credentials and auto-refresh. | `SYSTEM$VERIFY_CATALOG_INTEGRATION` output, and the refresh lag after a Spark commit. |
| `22_catalog_linked_database.sql` | Automatic table discovery across a namespace. | Discovery time and `SYSTEM$CATALOG_LINK_STATUS` output. |

Then run `50_checks.sql` and compare:
```bash
python -m lab.compare lab.orders --snowflake "<row_count>,<min>,<max>,<sum>"
```

## 6. Change the table in Spark, watch Snowflake

```text
aws-demo step=evolve     # add / rename / widen columns, months -> days partitioning
  -> 50_checks.sql       # new schema? old rows NULL in channel? pruning across both specs?
aws-demo step=maintain   # compaction + snapshot expiry
  -> 50_checks.sql       # same fingerprint, fewer files; time travel to an expired snapshot fails
```

**Record** every difference between what Spark reports and what Snowflake
reports. Each one becomes a finding.

## 7. The other direction: Snowflake owns the table

Run `30_snowflake_managed_iceberg.sql`, take `metadataLocation`, then:
```bash
AWS_PROFILE=<profile-with-bucket-read> AWS_REGION=<REGION> \
  python -m lab.read_metadata s3://<BUCKET>/snowflake_managed/orders_sf/metadata/<file>.metadata.json
```
**Record:** field IDs and types as PyIceberg sees them (NUMBER → decimal or
long?), and whether the rename shows up correctly.

## 8. Performance and cost

Run `60_cost_compare.sql` in Snowflake and `spark/benchmark.sql` in Spark on
the same table. Record elapsed time, bytes scanned, partitions scanned and
credits used. Keep it small; it is a method demo, not a benchmark claim.

## 9. Tear down (trial credits and AWS cost)

```sql
DROP DATABASE IF EXISTS POLARIS_LINKED; DROP DATABASE IF EXISTS LAKEHOUSE_LAB;
DROP CATALOG INTEGRATION IF EXISTS LAB_POLARIS; DROP CATALOG INTEGRATION IF EXISTS LAB_OBJECT_STORE;
DROP EXTERNAL VOLUME IF EXISTS LAB_S3_VOLUME; DROP WAREHOUSE IF EXISTS LAB_WH;
```
```bash
aws s3 rm s3://<BUCKET> --recursive && aws s3api delete-bucket --bucket <BUCKET>
docker compose -f deploy/vps/docker-compose.yml down
```
Delete the IAM user's access key last.
