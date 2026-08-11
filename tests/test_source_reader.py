from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from etl_framework.ingestion import SparkSourceReader, SourceType
from etl_framework.metadata.models import SourceConfiguration


def source_config(
    source_type: str,
    path: str = "unused",
    *,
    options: dict[str, object] | None = None,
    query: str | None = None,
    source_filter: str | None = None,
) -> SourceConfiguration:
    return SourceConfiguration(
        source_configuration_id=1,
        dataset_id=1,
        source_query=query,
        load_type="FULL",
        batch_size=None,
        fetch_size=500,
        source_filter=source_filter,
        landing_path=path,
        file_format=source_type,
        options=options or {},
    )


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("source-reader-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_reads_csv_and_json_into_dataframes(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> None:
    root = tmp_path_factory.mktemp("sources")
    csv_path = root / "customers.csv"
    json_path = root / "customers.json"
    csv_path.write_text("id,name\n1,Ada\n2,Linus\n", encoding="utf-8")
    json_path.write_text(
        '{"id":1,"name":"Ada"}\n{"id":2,"name":"Linus"}\n',
        encoding="utf-8",
    )
    reader = SparkSourceReader(spark)

    csv_frame = reader.read(
        source_config("CSV", str(csv_path), source_filter="id >= 2")
    )
    json_frame = reader.read(source_config("JSON", str(json_path)))

    assert csv_frame.count() == 1
    assert json_frame.count() == 2
    assert reader.validate_connectivity(source_config("CSV", str(csv_path))).success


class FakeFrame:
    def filter(self, expression: str) -> "FakeFrame":
        return self

    def limit(self, count: int) -> "FakeFrame":
        return self

    def count(self) -> int:
        return 1


class FakeDataFrameReader:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.read_options: dict[str, str] = {}
        self.loaded_path: str | None = None

    def schema(self, schema):
        return self

    def format(self, format_name: str) -> "FakeDataFrameReader":
        self.format_name = format_name
        return self

    def options(self, **options: str) -> "FakeDataFrameReader":
        self.read_options.update(options)
        return self

    def load(self, path: str | None = None) -> FakeFrame:
        self.loaded_path = path
        return FakeFrame()


class FakeSpark:
    def __init__(self) -> None:
        self.read = FakeDataFrameReader()


def test_configures_sql_jdbc_reader_and_runtime_credentials() -> None:
    spark = FakeSpark()
    reader = SparkSourceReader(spark)
    configuration = source_config(
        "SQL",
        options={
            "url": "jdbc:sqlserver://example:1433;database=Sample",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "dbtable": "dbo.Customer",
        },
        query="SELECT CustomerId FROM dbo.Customer",
    )

    frame = reader.read(
        configuration, runtime_options={"user": "runtime-user", "password": "secret"}
    )

    assert isinstance(frame, FakeFrame)
    assert spark.read.format_name == "jdbc"
    assert spark.read.read_options["query"] == configuration.source_query
    assert "dbtable" not in spark.read.read_options
    assert spark.read.read_options["fetchsize"] == "500"
    assert spark.read.read_options["password"] == "secret"


def test_configures_parquet_reader() -> None:
    spark = FakeSpark()

    frame = SparkSourceReader(spark).read(
        source_config("PARQUET", "abfss://raw/products", options={"mergeSchema": True})
    )

    assert isinstance(frame, FakeFrame)
    assert spark.read.format_name == "parquet"
    assert spark.read.loaded_path == "abfss://raw/products"
    assert spark.read.read_options["mergeSchema"] == "true"


def test_connectivity_failure_is_safe_and_does_not_expose_secrets() -> None:
    reader = SparkSourceReader(FakeSpark())

    result = reader.validate_connectivity(
        source_config("SQL"), runtime_options={"password": "do-not-expose"}
    )

    assert not result.success
    assert result.source_type is SourceType.SQL
    assert "do-not-expose" not in result.message


def test_rejects_unknown_source_type() -> None:
    result = SparkSourceReader(FakeSpark()).validate_connectivity(
        source_config("XML")
    )

    assert not result.success
    assert result.source_type is None
