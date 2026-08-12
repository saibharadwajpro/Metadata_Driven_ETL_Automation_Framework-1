"""Azure SQL metadata repository reader."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from contextlib import closing
from decimal import Decimal
from typing import Any, Protocol

from .errors import MetadataNotFoundError
from .models import (
    ColumnMapping,
    DatasetColumn,
    PipelineMetadata,
    PipelineStep,
    SourceConfiguration,
    TransformationRule,
    ValidationRule,
    WatermarkConfiguration,
)
from .validation import validate_metadata


class Cursor(Protocol):
    description: Iterable[tuple[Any, ...]]

    def execute(self, query: str, *parameters: Any) -> "Cursor": ...

    def fetchall(self) -> Iterable[Any]: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[[], Connection]


def pyodbc_connection_factory(connection_string: str) -> ConnectionFactory:
    """Build a lazy pyodbc connection factory without logging credentials."""
    if not connection_string.strip():
        raise ValueError("SQL connection string must not be empty")

    def connect() -> Connection:
        import pyodbc

        return pyodbc.connect(connection_string)

    return connect


def _json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Metadata JSON value must be an object")
    return parsed


class MetadataReader:
    """Read one consistent, active pipeline metadata manifest."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def load_pipeline_metadata(
        self, pipeline_code: str, environment_code: str, *, validate: bool = True
    ) -> PipelineMetadata:
        if not pipeline_code.strip() or not environment_code.strip():
            raise ValueError("pipeline_code and environment_code are required")

        with closing(self._connection_factory()) as connection:
            pipeline_rows = self._fetch_all(
                connection,
                """
                SELECT TOP (1) pipeline_id, pipeline_code, pipeline_name,
                       environment_code, metadata_version
                FROM meta.pipeline
                WHERE pipeline_code = ? AND environment_code = ?
                  AND is_active = 1
                  AND effective_from_utc <= SYSUTCDATETIME()
                  AND (effective_to_utc IS NULL OR effective_to_utc > SYSUTCDATETIME())
                ORDER BY metadata_version DESC
                """,
                pipeline_code,
                environment_code,
            )
            if not pipeline_rows:
                raise MetadataNotFoundError(
                    f"No active metadata for pipeline {pipeline_code!r} in {environment_code!r}"
                )

            pipeline = pipeline_rows[0]
            pipeline_id = int(pipeline["pipeline_id"])
            steps = self.read_pipeline_steps(connection, pipeline_id)
            step_ids = tuple(step.pipeline_step_id for step in steps)
            source_dataset_ids = tuple(
                step.source_dataset_id
                for step in steps
                if step.source_dataset_id is not None
            )
            dataset_ids = tuple(dict.fromkeys(
                dataset_id
                for step in steps
                for dataset_id in (step.source_dataset_id, step.target_dataset_id)
                if dataset_id is not None
            ))

            metadata = PipelineMetadata(
                pipeline_id=pipeline_id,
                pipeline_code=str(pipeline["pipeline_code"]),
                pipeline_name=str(pipeline["pipeline_name"]),
                environment_code=str(pipeline["environment_code"]),
                metadata_version=int(pipeline["metadata_version"]),
                steps=steps,
                source_configurations=self.read_source_configurations(
                    connection, source_dataset_ids
                ),
                column_mappings=self.read_column_mappings(connection, step_ids),
                transformation_rules=self.read_transformation_rules(connection, step_ids),
                validation_rules=self.read_validation_rules(connection, step_ids),
                watermark_configurations=self.read_watermark_configurations(
                    connection, step_ids
                ),
                dataset_columns=self.read_dataset_columns(connection, dataset_ids),
            )

        if validate:
            validate_metadata(metadata)
        return metadata

    def read_pipeline_steps(
        self, connection: Connection, pipeline_id: int
    ) -> tuple[PipelineStep, ...]:
        rows = self._fetch_all(
            connection,
            """
            SELECT pipeline_step_id, step_code, step_name, step_type,
                   execution_order, source_dataset_id, target_dataset_id,
                   load_type, write_strategy, watermark_column, retry_count,
                   retry_interval_sec, timeout_sec, config_json
            FROM meta.pipeline_step
            WHERE pipeline_id = ? AND is_active = 1
            ORDER BY execution_order
            """,
            pipeline_id,
        )
        return tuple(
            PipelineStep(
                pipeline_step_id=int(row["pipeline_step_id"]),
                step_code=str(row["step_code"]),
                step_name=str(row["step_name"]),
                step_type=str(row["step_type"]),
                execution_order=int(row["execution_order"]),
                source_dataset_id=row["source_dataset_id"],
                target_dataset_id=row["target_dataset_id"],
                load_type=row["load_type"],
                write_strategy=row["write_strategy"],
                watermark_column=row["watermark_column"],
                retry_count=int(row["retry_count"]),
                retry_interval_sec=int(row["retry_interval_sec"]),
                timeout_sec=row["timeout_sec"],
                config=_json_object(row["config_json"]),
            )
            for row in rows
        )

    def read_source_configurations(
        self, connection: Connection, dataset_ids: tuple[int, ...]
    ) -> tuple[SourceConfiguration, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT source_configuration_id, dataset_id, source_query, load_type,
                      batch_size, fetch_size, source_filter, landing_path,
                      file_format, options_json
               FROM meta.source_configuration
               WHERE is_active = 1 AND dataset_id IN ({})
               ORDER BY dataset_id""",
            dataset_ids,
        )
        return tuple(
            SourceConfiguration(
                source_configuration_id=int(r["source_configuration_id"]),
                dataset_id=int(r["dataset_id"]),
                source_query=r["source_query"],
                load_type=str(r["load_type"]),
                batch_size=r["batch_size"],
                fetch_size=r["fetch_size"],
                source_filter=r["source_filter"],
                landing_path=str(r["landing_path"]),
                file_format=str(r["file_format"]),
                options=_json_object(r["options_json"]),
            )
            for r in rows
        )

    def read_dataset_columns(
        self, connection: Connection, dataset_ids: tuple[int, ...]
    ) -> tuple[DatasetColumn, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT dataset_column_id, dataset_id, column_name,
                      ordinal_position, data_type, is_nullable,
                      is_business_key, is_partition_column,
                      sensitivity_class, default_expression
               FROM meta.dataset_column
               WHERE dataset_id IN ({})
               ORDER BY dataset_id, ordinal_position""",
            dataset_ids,
        )
        return tuple(DatasetColumn(
            dataset_column_id=int(r["dataset_column_id"]),
            dataset_id=int(r["dataset_id"]), column_name=str(r["column_name"]),
            ordinal_position=int(r["ordinal_position"]), data_type=str(r["data_type"]),
            is_nullable=bool(r["is_nullable"]), is_business_key=bool(r["is_business_key"]),
            is_partition_column=bool(r["is_partition_column"]),
            sensitivity_class=r["sensitivity_class"], default_expression=r["default_expression"],
        ) for r in rows)

    def read_column_mappings(
        self, connection: Connection, step_ids: tuple[int, ...]
    ) -> tuple[ColumnMapping, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT column_mapping_id, pipeline_step_id, mapping_order,
                      source_column, target_column, transform_expression,
                      target_data_type
               FROM meta.column_mapping
               WHERE is_active = 1 AND pipeline_step_id IN ({})
               ORDER BY pipeline_step_id, mapping_order""",
            step_ids,
        )
        return tuple(ColumnMapping(**dict(r)) for r in rows)

    def read_transformation_rules(
        self, connection: Connection, step_ids: tuple[int, ...]
    ) -> tuple[TransformationRule, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT transformation_rule_id, pipeline_step_id, rule_code,
                      rule_order, rule_type, rule_expression, rule_description
               FROM meta.transformation_rule
               WHERE is_active = 1 AND pipeline_step_id IN ({})
               ORDER BY pipeline_step_id, rule_order""",
            step_ids,
        )
        return tuple(TransformationRule(**dict(r)) for r in rows)

    def read_validation_rules(
        self, connection: Connection, step_ids: tuple[int, ...]
    ) -> tuple[ValidationRule, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT validation_rule_id, pipeline_step_id, rule_code, rule_type,
                      rule_expression, severity, threshold_value
               FROM meta.validation_rule
               WHERE is_active = 1 AND pipeline_step_id IN ({})
               ORDER BY pipeline_step_id, validation_rule_id""",
            step_ids,
        )
        return tuple(
            ValidationRule(
                **{
                    **dict(r),
                    "threshold_value": (
                        float(r["threshold_value"])
                        if isinstance(r["threshold_value"], Decimal)
                        else r["threshold_value"]
                    ),
                }
            )
            for r in rows
        )

    def read_watermark_configurations(
        self, connection: Connection, step_ids: tuple[int, ...]
    ) -> tuple[WatermarkConfiguration, ...]:
        rows = self._fetch_by_ids(
            connection,
            """SELECT watermark_configuration_id, pipeline_step_id,
                      watermark_name, watermark_column, value_data_type,
                      initial_value, comparison_operator, lookback_value
               FROM meta.watermark_configuration
               WHERE is_active = 1 AND pipeline_step_id IN ({})
               ORDER BY pipeline_step_id, watermark_name""",
            step_ids,
        )
        return tuple(WatermarkConfiguration(**dict(r)) for r in rows)

    def _fetch_by_ids(
        self,
        connection: Connection,
        query_template: str,
        identifiers: tuple[int, ...],
    ) -> list[Mapping[str, Any]]:
        if not identifiers:
            return []
        placeholders = ",".join("?" for _ in identifiers)
        return self._fetch_all(connection, query_template.format(placeholders), *identifiers)

    @staticmethod
    def _fetch_all(
        connection: Connection, query: str, *parameters: Any
    ) -> list[Mapping[str, Any]]:
        with closing(connection.cursor()) as cursor:
            cursor.execute(query, *parameters)
            columns = [str(column[0]) for column in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
