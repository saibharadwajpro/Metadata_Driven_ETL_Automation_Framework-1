"""Compare Spark schemas with expected dataset-column metadata."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from etl_framework.metadata.models import DatasetColumn

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


_ALIASES = {
    "BOOL": "BOOLEAN", "BYTE": "TINYINT", "SHORT": "SMALLINT",
    "INT": "INTEGER", "LONG": "BIGINT", "VARCHAR": "STRING",
    "CHAR": "STRING", "DATETIME": "TIMESTAMP",
}


def _type_name(value: str) -> str:
    normalized = "".join(value.strip().upper().split())
    if normalized.startswith(("VARCHAR(", "CHAR(")):
        return "STRING"
    return _ALIASES.get(normalized, normalized)


@dataclass(frozen=True)
class SchemaIssue:
    category: str
    column_name: str
    expected: str | None
    actual: str | None
    message: str


@dataclass(frozen=True)
class SchemaValidationReport:
    dataset_id: int
    is_valid: bool
    expected_column_count: int
    actual_column_count: int
    issues: tuple[SchemaIssue, ...]

    @property
    def missing_columns(self) -> tuple[str, ...]:
        return tuple(i.column_name for i in self.issues if i.category == "MISSING_COLUMN")

    @property
    def unexpected_columns(self) -> tuple[str, ...]:
        return tuple(i.column_name for i in self.issues if i.category == "UNEXPECTED_COLUMN")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "is_valid": self.is_valid,
            "expected_column_count": self.expected_column_count,
            "actual_column_count": self.actual_column_count,
            "issues": [asdict(issue) for issue in self.issues],
        }


class SchemaValidator:
    """Validate names and types from the DataFrame schema without reading rows."""

    def validate_dataframe(
        self, dataframe: DataFrame, expected_columns: tuple[DatasetColumn, ...] | list[DatasetColumn],
        *, allow_unexpected_columns: bool = False, case_sensitive: bool = False,
    ) -> SchemaValidationReport:
        return self.validate_schema(
            dataframe.schema, expected_columns,
            allow_unexpected_columns=allow_unexpected_columns,
            case_sensitive=case_sensitive,
        )

    def validate_schema(
        self, source_schema: StructType, expected_columns: tuple[DatasetColumn, ...] | list[DatasetColumn],
        *, allow_unexpected_columns: bool = False, case_sensitive: bool = False,
    ) -> SchemaValidationReport:
        if not expected_columns:
            raise ValueError("Expected schema metadata must contain at least one column")
        dataset_ids = {column.dataset_id for column in expected_columns}
        if len(dataset_ids) != 1:
            raise ValueError("Expected columns must belong to exactly one dataset")

        key = (lambda name: name) if case_sensitive else (lambda name: name.casefold())
        expected = {key(column.column_name): column for column in expected_columns}
        actual = {key(field.name): field for field in source_schema.fields}
        if len(expected) != len(expected_columns):
            raise ValueError("Expected schema contains duplicate column names")
        if len(actual) != len(source_schema.fields):
            raise ValueError("Source schema contains duplicate column names")
        issues: list[SchemaIssue] = []

        for normalized, column in expected.items():
            field = actual.get(normalized)
            if field is None:
                issues.append(SchemaIssue("MISSING_COLUMN", column.column_name, _type_name(column.data_type), None, f"Required column {column.column_name!r} is missing"))
                continue
            if field.name != column.column_name:
                issues.append(SchemaIssue("COLUMN_NAME_CASE", column.column_name, column.column_name, field.name, f"Column {field.name!r} differs from expected casing {column.column_name!r}"))
            expected_type, actual_type = _type_name(column.data_type), _type_name(field.dataType.simpleString())
            if expected_type != actual_type:
                issues.append(SchemaIssue("TYPE_MISMATCH", column.column_name, expected_type, actual_type, f"Column {column.column_name!r} expected {expected_type} but found {actual_type}"))
            if not column.is_nullable and field.nullable:
                issues.append(SchemaIssue("NULLABILITY_MISMATCH", column.column_name, "NOT NULL", "NULLABLE", f"Column {column.column_name!r} must not be nullable"))

        if not allow_unexpected_columns:
            for normalized, field in actual.items():
                if normalized not in expected:
                    issues.append(SchemaIssue("UNEXPECTED_COLUMN", field.name, None, _type_name(field.dataType.simpleString()), f"Unexpected column {field.name!r} is present"))

        return SchemaValidationReport(next(iter(dataset_ids)), not issues, len(expected), len(actual), tuple(issues))
