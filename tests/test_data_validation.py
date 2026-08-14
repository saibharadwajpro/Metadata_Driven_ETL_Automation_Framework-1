from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

from etl_framework.metadata.models import DatasetColumn, ValidationRule
from etl_framework.validation import DataValidationError, DataValidator


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("data-validation-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def column(position: int, name: str, data_type: str, *, nullable: bool = True, key: bool = False) -> DatasetColumn:
    return DatasetColumn(position, 20, name, position, data_type, nullable, key, False, None, None)


def rule(rule_id: int, code: str, rule_type: str, expression: str, severity: str = "ERROR") -> ValidationRule:
    return ValidationRule(rule_id, 10, code, rule_type, expression, severity, 0)


def test_validates_and_separates_rejected_records(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("1", "a@example.com", "30"),
            ("1", "b@example.com", "31"),
            (None, "missing@example.com", "20"),
            ("2", "good@example.com", "bad-age"),
            ("3", "invalid-email", "40"),
            ("4", "ok@example.com", "25"),
        ],
        ["customer_id", "email", "age"],
    )
    result = DataValidator().validate(
        dataframe,
        pipeline_step_id=10,
        expected_columns=[
            column(1, "customer_id", "INTEGER", nullable=False, key=True),
            column(2, "email", "STRING", nullable=False),
            column(3, "age", "INTEGER"),
        ],
        validation_rules=[
            rule(1, "AGE_RANGE", "RANGE", "try_cast(age as int) BETWEEN 18 AND 120"),
            rule(2, "EMAIL_FORMAT", "REGEX", "email LIKE '%@%.%'", "WARNING"),
            rule(3, "EMAIL_REQUIRED", "NOT_NULL", "email IS NOT NULL"),
        ],
    )

    assert result.report.total_row_count == 6
    assert result.report.accepted_row_count == 2
    assert result.report.rejected_row_count == 4
    assert {row.customer_id for row in result.accepted.collect()} == {"3", "4"}
    rejected = {
        row.customer_id: row["__validation_errors"]
        for row in result.rejected.collect()
    }
    assert "DUPLICATE_BUSINESS_KEY" in rejected["1"]
    assert "MANDATORY_customer_id" in rejected[None]
    assert "TYPE_age" in rejected["2"]
    warning = next(item for item in result.report.rule_results if item.rule_code == "EMAIL_FORMAT")
    assert warning.failed_row_count == 1


def test_unique_rule_detects_duplicate_records(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame([("a",), ("a",), ("b",)], ["email"])
    result = DataValidator().validate(
        dataframe,
        pipeline_step_id=10,
        expected_columns=[column(1, "email", "STRING")],
        validation_rules=[rule(1, "UNIQUE_EMAIL", "UNIQUE", "email")],
    )

    assert result.report.rejected_row_count == 2
    assert result.report.accepted_row_count == 1


def test_rejects_missing_expected_dataframe_column(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(DataValidationError, match="missing expected columns: email"):
        DataValidator().validate(
            dataframe,
            pipeline_step_id=10,
            expected_columns=[column(1, "email", "STRING")],
        )
