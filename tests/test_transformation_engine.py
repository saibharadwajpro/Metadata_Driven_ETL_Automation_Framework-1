from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from etl_framework.metadata.models import ColumnMapping, DatasetColumn, TransformationRule
from etl_framework.transformation import TransformationEngine, TransformationError


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transformation-engine-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def mapping(order: int, source: str | None, target: str, expression: str | None = None, data_type: str | None = None) -> ColumnMapping:
    return ColumnMapping(order, 10, order, source, target, expression, data_type)


def target(position: int, name: str, data_type: str, *, nullable: bool = True, default: str | None = None) -> DatasetColumn:
    return DatasetColumn(position, 20, name, position, data_type, nullable, False, False, None, default)


def rule(order: int, code: str, rule_type: str, expression: str) -> TransformationRule:
    return TransformationRule(order, 10, code, order, rule_type, expression, None)


def test_maps_casts_defaults_filters_derives_and_prepares_load_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [(1, "  Ada  ", None, True), (2, "Linus", "LINUS@EXAMPLE.COM", False)],
        ["id", "name", "email", "active"],
    )
    result = TransformationEngine().transform(
        source,
        pipeline_step_id=10,
        transformation_rules=[
            rule(1, "ACTIVE_ONLY", "FILTER", "active = true"),
            rule(2, "NORMALIZE_NAME", "DERIVE", "clean_name = trim(name)"),
        ],
        column_mappings=[
            mapping(1, "id", "customer_id", data_type="bigint"),
            mapping(2, "clean_name", "customer_name", data_type="string"),
            mapping(3, None, "email", "lower(email)", "string"),
        ],
        target_columns=[
            target(1, "customer_id", "BIGINT", nullable=False),
            target(2, "customer_name", "STRING", nullable=False),
            target(3, "email", "STRING", nullable=False, default="'unknown@example.com'"),
        ],
    )

    rows = result.dataframe.collect()
    assert result.dataframe.columns == ["customer_id", "customer_name", "email"]
    assert rows[0].asDict() == {
        "customer_id": 1,
        "customer_name": "Ada",
        "email": "unknown@example.com",
    }
    assert result.report.applied_rule_codes == ("ACTIVE_ONLY", "NORMALIZE_NAME")
    assert result.report.defaulted_columns == ("email",)


def test_deduplicates_using_business_rule(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [(1, "old", 1), (1, "new", 2), (2, "only", 1)],
        ["id", "value", "version"],
    )
    result = TransformationEngine().transform(
        source,
        pipeline_step_id=10,
        transformation_rules=[rule(1, "LATEST", "DEDUPLICATE", "id ORDER BY version DESC")],
        column_mappings=[mapping(1, "id", "id"), mapping(2, "value", "value")],
        null_policy="ALLOW",
    )

    assert {(row.id, row.value) for row in result.dataframe.collect()} == {(1, "new"), (2, "only")}


def test_null_policy_error_and_drop(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1,), (None,)], "id int")
    mappings = [mapping(1, "id", "id", data_type="integer")]
    targets = [target(1, "id", "INTEGER", nullable=False)]

    with pytest.raises(TransformationError, match="contain null values"):
        TransformationEngine().transform(
            source, pipeline_step_id=10, column_mappings=mappings, target_columns=targets
        )

    result = TransformationEngine().transform(
        source,
        pipeline_step_id=10,
        column_mappings=mappings,
        target_columns=targets,
        null_policy="DROP",
    )
    assert result.dataframe.count() == 1


def test_rejects_unsupported_business_rule(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(TransformationError, match="Unsupported executable"):
        TransformationEngine().transform(
            source,
            pipeline_step_id=10,
            column_mappings=[mapping(1, "id", "id")],
            transformation_rules=[rule(1, "JOIN_DATA", "JOIN", "other ON id")],
        )

