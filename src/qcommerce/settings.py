"""Runtime configuration.

Every component reads its settings from environment variables (optionally via a ``.env`` file),
so the same code runs unchanged on a laptop, in Docker Compose and in CI. ``.env.example``
documents every variable together with its default.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _config(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(env_prefix=prefix, env_file=".env", env_file_encoding="utf-8", extra="ignore")


class PostgresSettings(BaseSettings):
    """The OLTP source database that Debezium captures."""

    model_config = _config("QC_PG_")

    host: str = "localhost"
    port: int = 5432
    database: str = "qcommerce"
    app_schema: str = "commerce"
    # Owner/admin connection used for migrations, role and publication management.
    admin_user: str = "postgres"
    admin_password: SecretStr = SecretStr("postgres")
    # Least-privilege role used by the generator and reconciliation.
    app_user: str = "qc_app"
    app_password: SecretStr = SecretStr("qc_app")
    # Replication role used by Debezium.
    debezium_user: str = "debezium"
    debezium_password: SecretStr = SecretStr("debezium")
    publication: str = "qc_cdc_pub"
    slot: str = "qc_cdc_slot"

    def dsn(self, *, admin: bool = False) -> str:
        user, password = (
            (self.admin_user, self.admin_password) if admin else (self.app_user, self.app_password)
        )
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={user} password={password.get_secret_value()} application_name=qcommerce"
        )


class KafkaSettings(BaseSettings):
    model_config = _config("QC_KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "qc"

    @property
    def cdc_topic_pattern(self) -> str:
        """Regex matching every per-table CDC topic (``<prefix>.<schema>.<table>``)."""
        return rf"{self.topic_prefix}\.commerce\..+"


class ConnectSettings(BaseSettings):
    model_config = _config("QC_CONNECT_")

    url: str = "http://localhost:8083"
    connector_name: str = "qc-oltp-postgres"
    # How Kafka Connect reaches Postgres (differs from QC_PG_HOST when the CLI runs on the host).
    db_host: str | None = None
    db_port: int | None = None
    topic_partitions_hot: int = 3


class LakehouseSettings(BaseSettings):
    """Iceberg catalog (JDBC, backed by the platform Postgres) and object storage (S3 API)."""

    model_config = _config("QC_LAKE_")

    catalog: str = "lakehouse"
    catalog_uri: str = "jdbc:postgresql://localhost:5433/iceberg_catalog"
    catalog_user: str = "iceberg"
    catalog_password: SecretStr = SecretStr("iceberg")
    warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://localhost:8333"
    s3_access_key: SecretStr = SecretStr("lakehouse")
    s3_secret_key: SecretStr = SecretStr("lakehouse-secret")
    s3_region: str = "us-east-1"
    bronze_namespace: str = "bronze"
    silver_namespace: str = "silver"


class WriterSettings(BaseSettings):
    """The Spark Structured Streaming job that lands CDC events in Iceberg."""

    model_config = _config("QC_WRITER_")

    spark_master: str = "local[4]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 8
    # Maven coordinates resolved at start-up when the jars are not already on Spark's classpath
    # (local development). The container image ships the jars, so it leaves this empty.
    jars_packages: str = ""
    checkpoint_location: str = "/tmp/qc/checkpoints/cdc-writer"  # noqa: S108 - override in every deployment
    trigger_seconds: int = 15
    max_offsets_per_trigger: int = 100000
    starting_offsets: str = "earliest"
    merge_retries: int = 5
    metrics_port: int = 9405


class TrinoSettings(BaseSettings):
    model_config = _config("QC_TRINO_")

    host: str = "localhost"
    port: int = 8080
    user: str = "qcommerce"
    catalog: str = "lakehouse"


class GeneratorSettings(BaseSettings):
    model_config = _config("QC_GEN_")

    seed: int = 42
    cities: int = 2
    stores_per_city: int = 3
    products: int = 320
    customers: int = 2500
    riders_per_store: int = 28
    base_orders_per_store_hour: float = 24.0
    promised_minutes: int = 10
    # Simulated transactions grouped into one database commit during backfills.
    backfill_txn_batch: int = 25


class Settings:
    """Lazily-constructed bundle of all settings groups."""

    @property
    def pg(self) -> PostgresSettings:
        return _pg()

    @property
    def kafka(self) -> KafkaSettings:
        return _kafka()

    @property
    def connect(self) -> ConnectSettings:
        return _connect()

    @property
    def lake(self) -> LakehouseSettings:
        return _lake()

    @property
    def writer(self) -> WriterSettings:
        return _writer()

    @property
    def trino(self) -> TrinoSettings:
        return _trino()

    @property
    def generator(self) -> GeneratorSettings:
        return _generator()


@lru_cache
def _pg() -> PostgresSettings:
    return PostgresSettings()


@lru_cache
def _kafka() -> KafkaSettings:
    return KafkaSettings()


@lru_cache
def _connect() -> ConnectSettings:
    return ConnectSettings()


@lru_cache
def _lake() -> LakehouseSettings:
    return LakehouseSettings()


@lru_cache
def _writer() -> WriterSettings:
    return WriterSettings()


@lru_cache
def _trino() -> TrinoSettings:
    return TrinoSettings()


@lru_cache
def _generator() -> GeneratorSettings:
    return GeneratorSettings()


@lru_cache
def get_settings() -> Settings:
    return Settings()
