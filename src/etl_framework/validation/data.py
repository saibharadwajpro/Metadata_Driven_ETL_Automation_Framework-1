"""Row-level, metadata-driven Spark data-quality validation."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from etl_framework.metadata.models import DatasetColumn, ValidationRule


class DataValidationError(ValueError):
    """Raised when validation metadata cannot be executed."""


@dataclass(frozen=True)
class RuleValidationResult:
    rule_code: str
    rule_type: str
    severity: str
    failed_row_count: int


@dataclass(frozen=True)
class DataValidationReport:
    pipeline_step_id: int
    total_row_count: int
    accepted_row_count: int
    rejected_row_count: int
    rule_results: tuple[RuleValidationResult, ...]

    @property
    def is_valid(self) -> bool:
        return self.rejected_row_count == 0


@dataclass(frozen=True)
class DataValidationResult:
    accepted: DataFrame
    rejected: DataFrame
    report: DataValidationReport


@dataclass(frozen=True)
class _Check:
    code: str
    rule_type: str
    severity: str
    failed_when: object


_TYPE_ALIASES = {
    "BOOL": "boolean", "BOOLEAN": "boolean", "BYTE": "tinyint",
    "SHORT": "smallint", "INT": "int", "INTEGER": "int",
    "LONG": "bigint", "STRING": "string", "VARCHAR": "string",
    "CHAR": "string", "DATETIME": "timestamp",
}


def _spark_type(value: str) -> str:
    normalized = "".join(value.strip().upper().split())
    if normalized.startswith(("VARCHAR(", "CHAR(")):
        return "string"
    return _TYPE_ALIASES.get(normalized, normalized.lower())


class DataValidator:
    """Validate records and split accepted rows from rejected rows."""

    def validate(
        self,
        dataframe: DataFrame,
        *,
        pipeline_step_id: int,
        expected_columns: Sequence[DatasetColumn],
        validation_rules: Sequence[ValidationRule] = (),
    ) -> DataValidationResult:
        columns = tuple(expected_columns)
        rules = tuple(rule for rule in validation_rules if rule.pipeline_step_id == pipeline_step_id)
        self._validate_metadata(dataframe, columns, rules)
        checks = self._build_checks(columns, rules)

        errors = [F.when(c.failed_when, F.lit(c.code)) for c in checks if c.severity == "ERROR"]
        warnings = [F.when(c.failed_when, F.lit(c.code)) for c in checks if c.severity in {"WARNING", "INFO"}]
        annotated = dataframe.withColumn(
            "__validation_errors", self._compact_array(errors)
        ).withColumn(
            "__validation_warnings", self._compact_array(warnings)
        )
        accepted = annotated.filter(F.size("__validation_errors") == 0).drop(
            "__validation_errors", "__validation_warnings"
        )
        rejected = annotated.filter(F.size("__validation_errors") > 0)

        total_count = annotated.count()
        rejected_count = rejected.count()
        failure_counts = self._failure_counts(annotated)
        report = DataValidationReport(
            pipeline_step_id, total_count, total_count - rejected_count, rejected_count,
            tuple(RuleValidationResult(c.code, c.rule_type, c.severity, failure_counts.get(c.code, 0)) for c in checks),
        )
        return DataValidationResult(accepted, rejected, report)

    @staticmethod
    def _compact_array(items: list[object]):
        if not items:
            return F.from_json(F.lit("[]"), "array<string>")
        return F.filter(F.array(*items), lambda value: value.isNotNull())

    @staticmethod
    def _failure_counts(dataframe: DataFrame) -> dict[str, int]:
        rows = dataframe.select(
            F.explode(F.concat("__validation_errors", "__validation_warnings")).alias("code")
        ).groupBy("code").count().collect()
        return {row.code: row["count"] for row in rows}

    def _build_checks(
        self, columns: tuple[DatasetColumn, ...], rules: tuple[ValidationRule, ...]
    ) -> list[_Check]:
        checks: list[_Check] = []
        for column in columns:
            value = F.col(column.column_name)
            if not column.is_nullable:
                checks.append(_Check(f"MANDATORY_{column.column_name}", "MANDATORY", "ERROR", value.isNull()))
            escaped = column.column_name.replace("`", "``")
            checks.append(_Check(
                f"TYPE_{column.column_name}", "DATA_TYPE", "ERROR",
                value.isNotNull() & F.expr(f"try_cast(`{escaped}` as {_spark_type(column.data_type)}) IS NULL"),
            ))

        business_keys = [column.column_name for column in columns if column.is_business_key]
        if business_keys:
            duplicate_count = F.count(F.lit(1)).over(Window.partitionBy(*business_keys))
            checks.append(_Check("DUPLICATE_BUSINESS_KEY", "UNIQUE", "ERROR", duplicate_count > 1))

        for rule in rules:
            rule_type, severity = rule.rule_type.strip().upper(), rule.severity.strip().upper()
            if rule_type == "UNIQUE":
                keys = [name.strip() for name in rule.rule_expression.split(",") if name.strip()]
                if not keys:
                    raise DataValidationError(f"UNIQUE rule {rule.rule_code!r} has no columns")
                failed = F.count(F.lit(1)).over(Window.partitionBy(*keys)) > 1
            elif rule_type in {"NOT_NULL", "RANGE", "REGEX", "CUSTOM_SQL", "REFERENTIAL"}:
                failed = ~F.coalesce(F.expr(rule.rule_expression).cast("boolean"), F.lit(False))
            elif rule_type == "ROW_COUNT":
                raise DataValidationError(f"ROW_COUNT rule {rule.rule_code!r} cannot reject individual rows")
            else:
                raise DataValidationError(f"Unsupported validation rule type {rule.rule_type!r}")
            checks.append(_Check(rule.rule_code, rule_type, severity, failed))
        return checks

    @staticmethod
    def _validate_metadata(
        dataframe: DataFrame, columns: tuple[DatasetColumn, ...], rules: tuple[ValidationRule, ...]
    ) -> None:
        if not columns:
            raise DataValidationError("Expected column metadata is required")
        missing = sorted(c.column_name for c in columns if c.column_name not in dataframe.columns)
        if missing:
            raise DataValidationError("DataFrame is missing expected columns: " + ", ".join(missing))
        duplicates = sorted(code for code, count in Counter(r.rule_code for r in rules).items() if count > 1)
        if duplicates:
            raise DataValidationError("Duplicate validation rule codes: " + ", ".join(duplicates))
        invalid = sorted({r.severity for r in rules if r.severity.upper() not in {"INFO", "WARNING", "ERROR"}})
        if invalid:
            raise DataValidationError("Invalid rule severities: " + ", ".join(invalid))
