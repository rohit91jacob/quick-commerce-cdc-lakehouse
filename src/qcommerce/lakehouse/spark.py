"""SparkSession factory wired to the Iceberg JDBC catalog and S3-compatible object storage."""

from __future__ import annotations

from pyspark.sql import SparkSession

from qcommerce.settings import Settings

ICEBERG_VERSION = "1.12.0"
SPARK_VERSION = "4.1.3"
# Maven coordinates for local development (the writer image ships these jars in $SPARK_HOME/jars).
LOCAL_PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",
        "org.postgresql:postgresql:42.7.13",
    ]
)

# Newer AWS SDKs add CRC checksums to every request by default; S3-compatible stores do not all
# support that, and Iceberg does not need it.
_AWS_SDK_FLAGS = (
    "-Daws.requestChecksumCalculation=WHEN_REQUIRED -Daws.responseChecksumValidation=WHEN_REQUIRED"
)


def build_session(settings: Settings, app_name: str) -> SparkSession:
    lake, writer = settings.lake, settings.writer
    c = f"spark.sql.catalog.{lake.catalog}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master(writer.spark_master)
        .config("spark.driver.memory", writer.driver_memory)
        .config("spark.driver.extraJavaOptions", _AWS_SDK_FLAGS)
        .config("spark.executor.extraJavaOptions", _AWS_SDK_FLAGS)
        .config("spark.sql.shuffle.partitions", str(writer.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"{c}.uri", lake.catalog_uri)
        .config(f"{c}.jdbc.user", lake.catalog_user)
        .config(f"{c}.jdbc.password", lake.catalog_password.get_secret_value())
        .config(f"{c}.jdbc.schema-version", "V1")
        .config(f"{c}.warehouse", lake.warehouse)
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", lake.s3_endpoint)
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", lake.s3_access_key.get_secret_value())
        .config(f"{c}.s3.secret-access-key", lake.s3_secret_key.get_secret_value())
        .config(f"{c}.client.region", lake.s3_region)
        .config(f"{c}.cache-enabled", "false")
    )
    if writer.jars_packages:
        builder = builder.config("spark.jars.packages", writer.jars_packages)
    return builder.getOrCreate()


def build_local_test_session(warehouse_dir: str, *, packages: str = LOCAL_PACKAGES) -> SparkSession:
    """A small session with an Iceberg Hadoop catalog on the local filesystem (unit tests)."""
    return (
        SparkSession.builder.appName("qc-tests")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse_dir}")
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false")
        .getOrCreate()
    )
