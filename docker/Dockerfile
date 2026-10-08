# syntax=docker/dockerfile:1.7
# Two images from one file:
#   --target app     generator, CDC tooling, reconciliation, maintenance, Dagster + dbt
#   --target writer  the Spark Structured Streaming CDC writer (Spark + Iceberg + Kafka jars baked in)
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java21-python3-ubuntu

# ------------------------------------------------------------------------------------------------ app
FROM python:3.12-slim-bookworm AS app
ENV PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /usr/local/bin/uv
WORKDIR /opt/qcommerce
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --extra app --no-install-project
COPY src ./src
COPY dbt ./dbt
RUN uv sync --frozen --no-dev --extra app
ENV PATH=/opt/qcommerce/.venv/bin:$PATH \
    QC_DBT_PROJECT_DIR=/opt/qcommerce/dbt \
    DBT_PROFILES_DIR=/opt/qcommerce/dbt
# The dbt manifest Dagster loads its assets from (parsing needs no warehouse connection).
RUN cd dbt && dbt parse --quiet
RUN useradd --create-home --uid 1000 qc && mkdir -p /opt/dagster && chown -R qc /opt/qcommerce /opt/dagster
USER qc
ENTRYPOINT ["qc"]
CMD ["--help"]

# --------------------------------------------------------------------------------------------- writer
FROM ${SPARK_IMAGE} AS writer
USER root
# Newer distro Pythons mark the system interpreter as externally managed (PEP 668); older pips ignore this.
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ARG ICEBERG_VERSION=1.12.0
ARG SPARK_VERSION=4.1.3
# Runtime jars from Maven Central, each verified against its published SHA-1.
COPY docker/fetch_jars.py /tmp/fetch_jars.py
RUN python3 /tmp/fetch_jars.py /opt/spark/jars \
      "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:${ICEBERG_VERSION}" \
      "org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}" \
      "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}" \
      "org.apache.spark:spark-token-provider-kafka-0-10_2.13:${SPARK_VERSION}" \
      "org.apache.kafka:kafka-clients:3.9.1" \
      "org.apache.commons:commons-pool2:2.12.1" \
      "org.postgresql:postgresql:42.7.13" \
    && rm /tmp/fetch_jars.py \
    && ln -s "$(ls /opt/spark/python/lib/py4j-*-src.zip)" /opt/spark/python/lib/py4j-src.zip
RUN (python3 -m pip --version >/dev/null 2>&1 || (apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*)) \
    && python3 -m pip install --no-cache-dir "pydantic>=2.8,<3" "pydantic-settings>=2.4,<3"
COPY pyproject.toml README.md LICENSE /opt/qcommerce/
COPY src /opt/qcommerce/src
RUN python3 -m pip install --no-cache-dir --no-deps /opt/qcommerce \
    && mkdir -p /data/checkpoints && chown -R spark /data
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip \
    QC_WRITER_CHECKPOINT_LOCATION=/data/checkpoints/cdc-writer \
    QC_WRITER_JARS_PACKAGES=""
USER spark
WORKDIR /opt/qcommerce
EXPOSE 9405
ENTRYPOINT ["python3", "-m", "qcommerce.cli"]
CMD ["writer", "run"]
