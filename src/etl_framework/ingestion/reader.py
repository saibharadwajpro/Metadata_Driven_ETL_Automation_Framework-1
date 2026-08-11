"""Metadata-configured readers that produce Spark DataFrames."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from etl_framework.metadata.models import SourceConfiguration

from .errors import SourceConfigurationError
from .models import ConnectivityResult, SourceType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


_FILE_DEFAULTS: dict[SourceType, dict[str, str]] = {
    SourceType.CSV: {
        "header": "true",
        "inferSchema": "true",
        "mode": "FAILFAST",
    },
    SourceType.JSON: {
        "multiLine": "false",
        "mode": "FAILFAST",
    },
    SourceType.PARQUET: {
        "mergeSchema": "false",
    },
}
_SQL_ALIASES = {"SQL", "JDBC", "TABLE", "AZURE_SQL", "SQL_SERVER"}
def _spark_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


class SparkSourceReader:
    """Read supported source types from one source-configuration record."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def read(
        self,
        configuration: SourceConfiguration,
        *,
        runtime_options: Mapping[str, Any] | None = None,
        schema: StructType | str | None = None,
    ) -> DataFrame:
        """Load source data into a Spark DataFrame.

        Runtime options are intended for secrets resolved at execution time and
        take precedence over non-secret metadata options.
        """
        source_type = self._source_type(configuration.file_format)
        options = self._options(configuration, source_type, runtime_options)
        reader = self._spark.read
        if schema is not None:
            reader = reader.schema(schema)

        if source_type in _FILE_DEFAULTS:
            if not configuration.landing_path.strip():
                raise SourceConfigurationError(
                    f"{source_type.value} source requires a path"
                )
            frame = reader.format(source_type.value.lower()).options(**options).load(
                configuration.landing_path
            )
        else:
            self._validate_sql_options(options)
            frame = reader.format("jdbc").options(**options).load()

        if configuration.source_filter:
            frame = frame.filter(configuration.source_filter)
        return frame

    def validate_connectivity(
        self,
        configuration: SourceConfiguration,
        *,
        runtime_options: Mapping[str, Any] | None = None,
        schema: StructType | str | None = None,
    ) -> ConnectivityResult:
        """Validate access and readability using a one-row Spark action."""
        try:
            source_type = self._source_type(configuration.file_format)
            self.read(
                configuration, runtime_options=runtime_options, schema=schema
            ).limit(1).count()
        except Exception:
            # Never return connector exceptions because they may embed credentials.
            try:
                source_type = self._source_type(configuration.file_format)
            except SourceConfigurationError:
                return ConnectivityResult(
                    source_type=None,
                    success=False,
                    message="Unsupported source type or invalid source configuration",
                )
            return ConnectivityResult(
                source_type=source_type,
                success=False,
                message=f"{source_type.value} source connectivity validation failed",
            )

        return ConnectivityResult(
            source_type=source_type,
            success=True,
            message=f"{source_type.value} source is reachable and readable",
        )

    @staticmethod
    def _source_type(file_format: str) -> SourceType:
        normalized = file_format.strip().upper()
        if normalized in _SQL_ALIASES:
            return SourceType.SQL
        try:
            return SourceType(normalized)
        except ValueError as error:
            supported = ", ".join(item.value for item in SourceType)
            raise SourceConfigurationError(
                f"Unsupported source type {file_format!r}; expected one of {supported}"
            ) from error

    @staticmethod
    def _options(
        configuration: SourceConfiguration,
        source_type: SourceType,
        runtime_options: Mapping[str, Any] | None,
    ) -> dict[str, str]:
        options = dict(_FILE_DEFAULTS.get(source_type, {}))
        options.update(
            {key: _spark_value(value) for key, value in configuration.options.items()}
        )
        if runtime_options:
            options.update(
                {key: _spark_value(value) for key, value in runtime_options.items()}
            )

        if source_type is SourceType.SQL:
            if configuration.source_query:
                options.pop("dbtable", None)
                options["query"] = configuration.source_query
            if configuration.fetch_size is not None:
                options.setdefault("fetchsize", str(configuration.fetch_size))
        return options

    @staticmethod
    def _validate_sql_options(options: Mapping[str, str]) -> None:
        missing = [name for name in ("url", "driver") if not options.get(name)]
        if not options.get("query") and not options.get("dbtable"):
            missing.append("query or dbtable")
        if missing:
            raise SourceConfigurationError(
                "SQL source is missing required options: " + ", ".join(missing)
            )
