"""Watermark filtering, merge/upsert processing, and safe state advancement."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol
from uuid import UUID

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from etl_framework.metadata.models import WatermarkConfiguration


class IncrementalLoadError(RuntimeError):
    """Raised when incremental metadata or state is invalid."""


@dataclass(frozen=True)
class WatermarkState:
    value: str
    from_successful_run: bool


@dataclass(frozen=True)
class IncrementalLoadResult:
    filtered: DataFrame
    merged: DataFrame
    previous_watermark: str
    next_watermark: str | None
    processed_row_count: int
    watermark_updated: bool


class MergeTarget(Protocol):
    def merge(self, dataframe: DataFrame, key_columns: Sequence[str]) -> DataFrame: ...


class _Cursor(Protocol):
    def execute(self, query: str, *parameters: Any) -> Any: ...
    def fetchone(self) -> Any: ...
    def close(self) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class WatermarkRepository:
    """Read and atomically update watermark state in Azure SQL."""

    def __init__(self, connection_factory: Callable[[], _Connection]) -> None:
        self._connection_factory = connection_factory

    def get_last_successful(
        self, configuration: WatermarkConfiguration
    ) -> WatermarkState:
        with closing(self._connection_factory()) as connection, closing(connection.cursor()) as cursor:
            cursor.execute(
                """
                SELECT TOP (1) w.watermark_value
                FROM audit.watermark AS w
                INNER JOIN audit.etl_run AS r ON r.etl_run_id = w.source_etl_run_id
                WHERE w.pipeline_step_id = ? AND w.watermark_name = ?
                  AND r.status = 'SUCCEEDED'
                ORDER BY w.updated_at_utc DESC
                """,
                configuration.pipeline_step_id,
                configuration.watermark_name,
            )
            row = cursor.fetchone()
        if row is None:
            return WatermarkState(configuration.initial_value, False)
        return WatermarkState(str(row[0]), True)

    def update_after_success(
        self,
        configuration: WatermarkConfiguration,
        value: str,
        etl_run_id: UUID | str,
    ) -> None:
        connection = self._connection_factory()
        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute(
                    "SELECT status FROM audit.etl_run WHERE etl_run_id = ?",
                    str(etl_run_id),
                )
                row = cursor.fetchone()
                if row is None or str(row[0]).upper() != "SUCCEEDED":
                    raise IncrementalLoadError(
                        "Watermark can only advance for a successful ETL run"
                    )
                cursor.execute(
                    """
                    MERGE audit.watermark AS target
                    USING (SELECT ? AS pipeline_step_id, ? AS watermark_name) AS source
                    ON target.pipeline_step_id = source.pipeline_step_id
                       AND target.watermark_name = source.watermark_name
                    WHEN MATCHED THEN UPDATE SET
                        watermark_value = ?, value_data_type = ?,
                        source_etl_run_id = ?, updated_at_utc = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN INSERT
                        (pipeline_step_id, watermark_name, watermark_value,
                         value_data_type, source_etl_run_id)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    configuration.pipeline_step_id,
                    configuration.watermark_name,
                    value,
                    configuration.value_data_type,
                    str(etl_run_id),
                    configuration.pipeline_step_id,
                    configuration.watermark_name,
                    value,
                    configuration.value_data_type,
                    str(etl_run_id),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


class DataFrameUpsertTarget:
    """Spark DataFrame merge target for reusable/testable upsert semantics."""

    def __init__(self, current: DataFrame, *, order_by: str | None = None) -> None:
        self.current = current
        self.order_by = order_by

    def merge(self, dataframe: DataFrame, key_columns: Sequence[str]) -> DataFrame:
        keys = tuple(key_columns)
        if not keys:
            raise IncrementalLoadError("At least one merge key is required")
        missing = sorted(set(keys) - set(dataframe.columns))
        if missing:
            raise IncrementalLoadError("Incoming data is missing merge keys: " + ", ".join(missing))
        if set(self.current.columns) != set(dataframe.columns):
            raise IncrementalLoadError("Current and incoming DataFrames must have matching columns")

        existing = self.current.select(*dataframe.columns).withColumn("__merge_source", F.lit(0))
        incoming = dataframe.withColumn("__merge_source", F.lit(1))
        order = [F.col("__merge_source").desc()]
        if self.order_by:
            if self.order_by not in dataframe.columns:
                raise IncrementalLoadError(f"Merge order column {self.order_by!r} does not exist")
            order.insert(0, F.col(self.order_by).desc_nulls_last())
        rank = F.row_number().over(Window.partitionBy(*keys).orderBy(*order))
        self.current = existing.unionByName(incoming).withColumn(
            "__merge_rank", rank
        ).filter(F.col("__merge_rank") == 1).drop("__merge_rank", "__merge_source")
        return self.current


class IncrementalLoadCoordinator:
    """Coordinate watermark filtering, target merge, and state update."""

    def __init__(self, watermark_repository: WatermarkRepository) -> None:
        self._watermarks = watermark_repository

    def filter_incremental(
        self,
        dataframe: DataFrame,
        configuration: WatermarkConfiguration,
        watermark_value: str,
    ) -> DataFrame:
        column_name = configuration.watermark_column
        if column_name not in dataframe.columns:
            raise IncrementalLoadError(f"Watermark column {column_name!r} does not exist")
        data_type = self._spark_type(configuration.value_data_type)
        boundary = F.lit(watermark_value).cast(data_type)
        if configuration.lookback_value:
            if data_type in {"date", "timestamp"}:
                boundary = boundary - F.expr(
                    f"INTERVAL {int(configuration.lookback_value)} DAYS"
                )
            elif data_type in {"tinyint", "smallint", "int", "bigint", "decimal", "double", "float"}:
                boundary = boundary - F.lit(configuration.lookback_value)
            else:
                raise IncrementalLoadError("Lookback is not supported for STRING watermarks")
        value = F.col(column_name).cast(data_type)
        operators = {
            ">": value > boundary,
            ">=": value >= boundary,
            "<": value < boundary,
            "<=": value <= boundary,
        }
        try:
            condition = operators[configuration.comparison_operator]
        except KeyError as error:
            raise IncrementalLoadError(
                f"Unsupported watermark operator {configuration.comparison_operator!r}"
            ) from error
        return dataframe.filter(value.isNotNull() & condition)

    def execute(
        self,
        dataframe: DataFrame,
        *,
        configuration: WatermarkConfiguration,
        target: MergeTarget,
        key_columns: Sequence[str],
        etl_run_id: UUID | str,
    ) -> IncrementalLoadResult:
        state = self._watermarks.get_last_successful(configuration)
        filtered = self.filter_incremental(dataframe, configuration, state.value)
        processed_count = filtered.count()
        if processed_count == 0:
            return IncrementalLoadResult(
                filtered, target.merge(filtered, key_columns), state.value,
                None, 0, False,
            )

        next_value_raw = filtered.agg(
            F.max(F.col(configuration.watermark_column)).alias("next_watermark")
        ).first()["next_watermark"]
        next_value = self._serialize(next_value_raw)
        merged = target.merge(filtered, key_columns)
        # This call occurs strictly after merge returns successfully.
        self._watermarks.update_after_success(configuration, next_value, etl_run_id)
        return IncrementalLoadResult(
            filtered, merged, state.value, next_value, processed_count, True
        )

    @staticmethod
    def _spark_type(value: str) -> str:
        aliases = {
            "INTEGER": "int", "INT": "int", "DECIMAL": "decimal(38,18)",
            "DATE": "date", "TIMESTAMP": "timestamp", "STRING": "string",
        }
        normalized = value.strip().upper()
        if normalized not in aliases:
            raise IncrementalLoadError(f"Unsupported watermark type {value!r}")
        return aliases[normalized]

    @staticmethod
    def _serialize(value: Any) -> str:
        if value is None:
            raise IncrementalLoadError("Unable to calculate the next watermark")
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return str(value)
