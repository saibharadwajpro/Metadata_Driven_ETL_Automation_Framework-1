from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from etl_framework.loading import (
    DataFrameUpsertTarget,
    IncrementalLoadCoordinator,
    WatermarkState,
)
from etl_framework.metadata.models import WatermarkConfiguration


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("incremental-loading-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def configuration(initial: str = "2", lookback: int = 0) -> WatermarkConfiguration:
    return WatermarkConfiguration(
        1, 10, "CUSTOMER_VERSION", "version", "INTEGER", initial, ">", lookback
    )


class FakeWatermarkRepository:
    def __init__(self, value: str = "2") -> None:
        self.value = value
        self.updates: list[tuple[str, str]] = []

    def get_last_successful(self, config: WatermarkConfiguration) -> WatermarkState:
        return WatermarkState(self.value, True)

    def update_after_success(self, config, value: str, etl_run_id: str) -> None:
        self.updates.append((value, etl_run_id))


def test_filters_merges_upserts_and_advances_watermark(spark: SparkSession) -> None:
    incoming = spark.createDataFrame(
        [(1, "ignored", 1), (3, "new-three", 3), (4, "new-four", 4)],
        ["id", "value", "version"],
    )
    existing = spark.createDataFrame(
        [(1, "one", 1), (3, "old-three", 2)],
        ["id", "value", "version"],
    )
    repository = FakeWatermarkRepository("2")
    target = DataFrameUpsertTarget(existing, order_by="version")

    result = IncrementalLoadCoordinator(repository).execute(
        incoming,
        configuration=configuration(),
        target=target,
        key_columns=["id"],
        etl_run_id="run-1",
    )

    assert result.processed_row_count == 2
    assert result.previous_watermark == "2"
    assert result.next_watermark == "4"
    assert result.watermark_updated
    assert repository.updates == [("4", "run-1")]
    assert {(r.id, r.value, r.version) for r in result.merged.collect()} == {
        (1, "one", 1), (3, "new-three", 3), (4, "new-four", 4)
    }


def test_lookback_refilters_previous_numeric_range(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["version"])
    coordinator = IncrementalLoadCoordinator(FakeWatermarkRepository())

    filtered = coordinator.filter_incremental(
        dataframe, configuration(lookback=2), "3"
    )

    assert {row.version for row in filtered.collect()} == {2, 3, 4}


def test_does_not_advance_watermark_when_merge_fails(spark: SparkSession) -> None:
    class FailingTarget:
        def merge(self, dataframe, key_columns):
            raise RuntimeError("target unavailable")

    repository = FakeWatermarkRepository("2")
    incoming = spark.createDataFrame([(3, "new", 3)], ["id", "value", "version"])

    with pytest.raises(RuntimeError, match="target unavailable"):
        IncrementalLoadCoordinator(repository).execute(
            incoming,
            configuration=configuration(),
            target=FailingTarget(),
            key_columns=["id"],
            etl_run_id="run-2",
        )

    assert repository.updates == []


def test_empty_increment_does_not_advance_watermark(spark: SparkSession) -> None:
    repository = FakeWatermarkRepository("10")
    incoming = spark.createDataFrame([(3, "old", 3)], ["id", "value", "version"])
    current = spark.createDataFrame([(1, "one", 1)], ["id", "value", "version"])

    result = IncrementalLoadCoordinator(repository).execute(
        incoming,
        configuration=configuration(),
        target=DataFrameUpsertTarget(current),
        key_columns=["id"],
        etl_run_id="run-3",
    )

    assert result.processed_row_count == 0
    assert not result.watermark_updated
    assert repository.updates == []

