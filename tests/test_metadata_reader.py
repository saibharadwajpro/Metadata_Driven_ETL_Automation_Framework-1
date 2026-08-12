from __future__ import annotations

import pytest

from etl_framework.metadata import (
    MetadataNotFoundError,
    MetadataReader,
    MetadataValidationError,
)


class DummyConnection:
    def close(self) -> None:
        pass


class FixtureReader(MetadataReader):
    def __init__(self, rows: dict[str, list[dict[str, object]]]) -> None:
        super().__init__(DummyConnection)
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def _fetch_all(self, connection, query: str, *parameters):
        normalized = " ".join(query.split())
        self.calls.append((normalized, parameters))
        for table in sorted(self.rows, key=len, reverse=True):
            if f"FROM {table}" in normalized:
                return self.rows[table]
        raise AssertionError(f"Unexpected query: {normalized}")


def valid_rows() -> dict[str, list[dict[str, object]]]:
    return {
        "meta.pipeline": [
            {
                "pipeline_id": 10,
                "pipeline_code": "CUSTOMER_INCREMENTAL",
                "pipeline_name": "Customer Incremental",
                "environment_code": "dev",
                "metadata_version": 1,
            }
        ],
        "meta.pipeline_step": [
            {
                "pipeline_step_id": 20,
                "step_code": "INGEST_CUSTOMER",
                "step_name": "Ingest Customer",
                "step_type": "INGEST",
                "execution_order": 1,
                "source_dataset_id": 30,
                "target_dataset_id": 31,
                "load_type": "INCREMENTAL",
                "write_strategy": "MERGE",
                "watermark_column": "ModifiedAt",
                "retry_count": 2,
                "retry_interval_sec": 60,
                "timeout_sec": 900,
                "config_json": '{"partitions": 4}',
            }
        ],
        "meta.source_configuration": [
            {
                "source_configuration_id": 40,
                "dataset_id": 30,
                "source_query": "SELECT * FROM dbo.Customer",
                "load_type": "INCREMENTAL",
                "batch_size": 1000,
                "fetch_size": 500,
                "source_filter": "IsDeleted = 0",
                "landing_path": "abfss://raw/customer",
                "file_format": "PARQUET",
                "options_json": '{"compression": "snappy"}',
            }
        ],
        "meta.dataset_column": [
            {
                "dataset_column_id": 41,
                "dataset_id": 30,
                "column_name": "CustomerId",
                "ordinal_position": 1,
                "data_type": "BIGINT",
                "is_nullable": False,
                "is_business_key": True,
                "is_partition_column": False,
                "sensitivity_class": None,
                "default_expression": None,
            }
        ],
        "meta.column_mapping": [
            {
                "column_mapping_id": 50,
                "pipeline_step_id": 20,
                "mapping_order": 1,
                "source_column": "CustomerId",
                "target_column": "customer_id",
                "transform_expression": "CAST(CustomerId AS BIGINT)",
                "target_data_type": "BIGINT",
            }
        ],
        "meta.transformation_rule": [
            {
                "transformation_rule_id": 60,
                "pipeline_step_id": 20,
                "rule_code": "ACTIVE_ONLY",
                "rule_order": 1,
                "rule_type": "FILTER",
                "rule_expression": "CustomerId IS NOT NULL",
                "rule_description": "Remove invalid identifiers",
            }
        ],
        "meta.validation_rule": [
            {
                "validation_rule_id": 70,
                "pipeline_step_id": 20,
                "rule_code": "ID_REQUIRED",
                "rule_type": "NOT_NULL",
                "rule_expression": "customer_id IS NOT NULL",
                "severity": "ERROR",
                "threshold_value": 0.0,
            }
        ],
        "meta.watermark_configuration": [
            {
                "watermark_configuration_id": 80,
                "pipeline_step_id": 20,
                "watermark_name": "CUSTOMER_MODIFIED_AT",
                "watermark_column": "ModifiedAt",
                "value_data_type": "TIMESTAMP",
                "initial_value": "1900-01-01T00:00:00Z",
                "comparison_operator": ">",
                "lookback_value": 5,
            }
        ],
    }


def test_loads_and_validates_complete_pipeline_metadata() -> None:
    reader = FixtureReader(valid_rows())

    metadata = reader.load_pipeline_metadata("CUSTOMER_INCREMENTAL", "dev")

    assert metadata.pipeline_id == 10
    assert metadata.steps[0].config == {"partitions": 4}
    assert metadata.source_configurations[0].options == {"compression": "snappy"}
    assert metadata.dataset_columns[0].column_name == "CustomerId"
    assert metadata.column_mappings[0].target_column == "customer_id"
    assert metadata.transformation_rules[0].rule_code == "ACTIVE_ONLY"
    assert metadata.validation_rules[0].severity == "ERROR"
    assert metadata.watermark_configurations[0].watermark_column == "ModifiedAt"
    assert reader.calls[0][1] == ("CUSTOMER_INCREMENTAL", "dev")


def test_rejects_incremental_step_without_watermark() -> None:
    rows = valid_rows()
    rows["meta.watermark_configuration"] = []

    with pytest.raises(MetadataValidationError, match="requires watermark configuration"):
        FixtureReader(rows).load_pipeline_metadata("CUSTOMER_INCREMENTAL", "dev")


def test_reports_missing_active_pipeline() -> None:
    rows = valid_rows()
    rows["meta.pipeline"] = []

    with pytest.raises(MetadataNotFoundError):
        FixtureReader(rows).load_pipeline_metadata("UNKNOWN", "dev")


def test_rejects_empty_identifiers_before_database_access() -> None:
    reader = FixtureReader(valid_rows())

    with pytest.raises(ValueError, match="required"):
        reader.load_pipeline_metadata("", "dev")

    assert reader.calls == []
