"""Typed metadata records returned by the repository reader."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PipelineStep:
    pipeline_step_id: int
    step_code: str
    step_name: str
    step_type: str
    execution_order: int
    source_dataset_id: int | None
    target_dataset_id: int | None
    load_type: str | None
    write_strategy: str | None
    watermark_column: str | None
    retry_count: int
    retry_interval_sec: int
    timeout_sec: int | None
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceConfiguration:
    source_configuration_id: int
    dataset_id: int
    source_query: str | None
    load_type: str
    batch_size: int | None
    fetch_size: int | None
    source_filter: str | None
    landing_path: str
    file_format: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetColumn:
    dataset_column_id: int
    dataset_id: int
    column_name: str
    ordinal_position: int
    data_type: str
    is_nullable: bool
    is_business_key: bool
    is_partition_column: bool
    sensitivity_class: str | None
    default_expression: str | None


@dataclass(frozen=True)
class ColumnMapping:
    column_mapping_id: int
    pipeline_step_id: int
    mapping_order: int
    source_column: str | None
    target_column: str
    transform_expression: str | None
    target_data_type: str | None


@dataclass(frozen=True)
class TransformationRule:
    transformation_rule_id: int
    pipeline_step_id: int
    rule_code: str
    rule_order: int
    rule_type: str
    rule_expression: str
    rule_description: str | None


@dataclass(frozen=True)
class ValidationRule:
    validation_rule_id: int
    pipeline_step_id: int
    rule_code: str
    rule_type: str
    rule_expression: str
    severity: str
    threshold_value: float | None


@dataclass(frozen=True)
class WatermarkConfiguration:
    watermark_configuration_id: int
    pipeline_step_id: int
    watermark_name: str
    watermark_column: str
    value_data_type: str
    initial_value: str
    comparison_operator: str
    lookback_value: int


@dataclass(frozen=True)
class PipelineMetadata:
    pipeline_id: int
    pipeline_code: str
    pipeline_name: str
    environment_code: str
    metadata_version: int
    steps: tuple[PipelineStep, ...]
    source_configurations: tuple[SourceConfiguration, ...]
    column_mappings: tuple[ColumnMapping, ...]
    transformation_rules: tuple[TransformationRule, ...]
    validation_rules: tuple[ValidationRule, ...]
    watermark_configurations: tuple[WatermarkConfiguration, ...]
    dataset_columns: tuple[DatasetColumn, ...] = ()
