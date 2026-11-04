"""SparkSession wired to Polaris, matching Polaris' documented Spark config.

Spark pulls the Iceberg runtime from Maven on first start, so the machine
needs internet access (GitHub Actions runners have it).
"""

from __future__ import annotations

import os

ICEBERG_VERSION = os.environ.get("ICEBERG_VERSION", "1.10.1")
SPARK_SCALA = "3.5_2.12"
CATALOG = "polaris"


def build_session(app_name: str = "iceberg-polaris-lab"):
    from pyspark.sql import SparkSession

    packages = ",".join(
        [
            f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_SCALA}:{ICEBERG_VERSION}",
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        ]
    )
    c = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(f"{c}.uri", os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog"))
        .config(f"{c}.warehouse", os.environ.get("POLARIS_CATALOG", "lab_catalog"))
        .config(f"{c}.credential", os.environ.get("POLARIS_CREDENTIAL", "root:s3cr3t"))
        .config(f"{c}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"{c}.token-refresh-enabled", "false")
        .config(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
        .config(f"{c}.client.region", os.environ.get("AWS_REGION", "us-west-2"))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


SPARK_ORDERS_DDL = """
    order_id BIGINT NOT NULL,
    customer_id INT,
    region STRING,
    status STRING,
    amount DECIMAL(10,2),
    order_date DATE,
    updated_at TIMESTAMP
"""

SPARK_ORDERS_STRUCT = (
    "order_id long, customer_id int, region string, status string, "
    "amount decimal(10,2), order_date date, updated_at timestamp"
)


def arrow_to_spark(spark, table):
    """Arrow -> Spark DataFrame without pandas (keeps Decimal and tz exact)."""
    return spark.createDataFrame(table.to_pylist(), schema=SPARK_ORDERS_STRUCT)


def spark_fingerprint(spark, ident: str, snapshot_id: int | None = None) -> dict:
    """Same four numbers as lab.checks.fingerprint, computed by Spark SQL."""
    at = f" VERSION AS OF {snapshot_id}" if snapshot_id else ""
    row = spark.sql(
        f"SELECT count(*) AS n, min(order_id) AS lo, max(order_id) AS hi, "
        f"CAST(sum(amount) AS DECIMAL(20,2)) AS total FROM {CATALOG}.{ident}{at}"
    ).collect()[0]
    return {
        "row_count": row["n"],
        "min_order_id": row["lo"],
        "max_order_id": row["hi"],
        "amount_sum": None if row["total"] is None else str(row["total"]),
    }
