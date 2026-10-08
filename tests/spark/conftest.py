from __future__ import annotations

import os
import shutil
import tempfile
import time
from collections.abc import Iterator

import pytest

pytest.importorskip("pyspark")

# PySpark converts collected timestamps with the driver's local zone; pin it so assertions are stable.
os.environ["TZ"] = "UTC"
time.tzset()

from pyspark.sql import SparkSession  # noqa: E402

from qcommerce.lakehouse.spark import build_local_test_session  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("no JVM available")
    warehouse = tempfile.mkdtemp(prefix="qc-iceberg-")
    session = build_local_test_session(warehouse)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
