"""Build the demo table polaris.lab.orders that Snowflake reads.

    python -m spark.load_demo --step create     # 10,000 rows, PARTITIONED BY months(order_date)
    python -m spark.load_demo --step evolve     # add/rename/widen columns, months -> days, +400 rows
    python -m spark.load_demo --step maintain   # compaction + snapshot expiry
    python -m spark.load_demo --step all

Run each step, then re-check in Snowflake (snowflake/50_checks.sql). The
point is to see each kind of change arrive on the Snowflake side.
After every step the Spark-side fingerprint is printed for lab.compare.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json

from lab.data import orders
from spark.session import CATALOG, SPARK_ORDERS_DDL, arrow_to_spark, spark_fingerprint


def _utc_now() -> str:
    # Iceberg procedure arguments must be literals, not current_timestamp().
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


TABLE = "lab.orders"
FQ = f"{CATALOG}.{TABLE}"


def create(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.lab")
    spark.sql(f"DROP TABLE IF EXISTS {FQ}")
    spark.sql(
        f"CREATE TABLE {FQ} ({SPARK_ORDERS_DDL}) USING iceberg "
        f"PARTITIONED BY (months(order_date)) "
        f"TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')"
    )
    arrow_to_spark(spark, orders(10_000)).writeTo(FQ).append()


def evolve(spark) -> None:
    from pyspark.sql import functions as F

    spark.sql(f"ALTER TABLE {FQ} ADD COLUMN channel STRING")
    spark.sql(f"ALTER TABLE {FQ} RENAME COLUMN status TO order_status")
    spark.sql(f"ALTER TABLE {FQ} ALTER COLUMN customer_id TYPE BIGINT")
    spark.sql(f"ALTER TABLE {FQ} REPLACE PARTITION FIELD months(order_date) WITH days(order_date)")
    new_rows = (
        arrow_to_spark(spark, orders(400, start_id=10_001))
        .withColumnRenamed("status", "order_status")
        .withColumn("customer_id", F.col("customer_id").cast("bigint"))
        .withColumn("channel", F.lit("web"))
    )
    new_rows.writeTo(FQ).append()


def maintain(spark) -> None:
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{TABLE}', options => map('min-input-files', '2'))"
    ).show(truncate=False)
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{TABLE}', "
        f"older_than => TIMESTAMP '{_utc_now()}', retain_last => 3)"
    ).show(truncate=False)


STEPS = {"create": create, "evolve": evolve, "maintain": maintain}


def main(argv: list[str] | None = None) -> None:
    from spark.session import build_session

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--step", choices=[*STEPS, "all"], required=True)
    args = p.parse_args(argv)

    spark = build_session("load-demo")
    for name in STEPS if args.step == "all" else [args.step]:
        STEPS[name](spark)
        fp = spark_fingerprint(spark, TABLE)
        files = spark.sql(f"SELECT count(*) AS n FROM {FQ}.files").collect()[0]["n"]
        print(json.dumps({"step": name, "data_files": files, "spark_fingerprint": fp}))
        print(
            "snowflake compare string:",
            ",".join(str(fp[k]) for k in ("row_count", "min_order_id", "max_order_id", "amount_sum")),
        )
    spark.stop()


if __name__ == "__main__":
    main()
