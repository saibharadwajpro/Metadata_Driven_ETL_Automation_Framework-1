"""Apply metadata-defined transformations to Spark DataFrames."""

from __future__ import annotations

import re
from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from etl_framework.metadata.models import (
    ColumnMapping,
    DatasetColumn,
    TransformationRule,
)

from .errors import TransformationError
from .models import TransformationReport, TransformationResult


_DERIVE = re.compile(r"^\s*([^=]+?)\s*=\s*(.+)$", re.DOTALL)
_CAST = re.compile(r"^\s*([^:]+?)\s*:\s*(.+)$")
_RENAME = re.compile(r"^\s*([^:]+?)\s*:\s*([^:]+?)\s*$")
_DEDUPLICATE = re.compile(
    r"^\s*(.+?)\s+ORDER\s+BY\s+([^\s]+)(?:\s+(ASC|DESC))?\s*$",
    re.IGNORECASE,
)


class TransformationEngine:
    """Execute safe, ordered transformation operations from trusted metadata."""

    def transform(
        self,
        dataframe: DataFrame,
        *,
        pipeline_step_id: int,
        column_mappings: Sequence[ColumnMapping],
        transformation_rules: Sequence[TransformationRule] = (),
        target_columns: Sequence[DatasetColumn] = (),
        null_policy: str = "ERROR",
    ) -> TransformationResult:
        mappings = sorted(
            (m for m in column_mappings if m.pipeline_step_id == pipeline_step_id),
            key=lambda item: item.mapping_order,
        )
        rules = sorted(
            (r for r in transformation_rules if r.pipeline_step_id == pipeline_step_id),
            key=lambda item: item.rule_order,
        )
        if not mappings:
            raise TransformationError("At least one column mapping is required")
        self._validate_unique_mappings(mappings)

        transformed = dataframe
        applied_rules: list[str] = []
        for rule in rules:
            transformed = self._apply_rule(transformed, rule)
            applied_rules.append(rule.rule_code)

        expressions = []
        for mapping in mappings:
            if mapping.transform_expression:
                expression = F.expr(mapping.transform_expression)
            elif mapping.source_column:
                if mapping.source_column not in transformed.columns:
                    raise TransformationError(
                        f"Mapping source column {mapping.source_column!r} does not exist"
                    )
                expression = F.col(mapping.source_column)
            else:
                expression = F.lit(None)
            if mapping.target_data_type:
                expression = expression.cast(mapping.target_data_type)
            expressions.append(expression.alias(mapping.target_column))
        transformed = transformed.select(*expressions)

        target_by_name = {column.column_name.casefold(): column for column in target_columns}
        defaulted: list[str] = []
        required: list[str] = []
        for mapping in mappings:
            target = target_by_name.get(mapping.target_column.casefold())
            if target is None:
                continue
            if target.default_expression:
                transformed = transformed.withColumn(
                    mapping.target_column,
                    F.coalesce(
                        F.col(mapping.target_column), F.expr(target.default_expression)
                    ),
                )
                defaulted.append(mapping.target_column)
            if not target.is_nullable:
                required.append(mapping.target_column)

        normalized_policy = null_policy.strip().upper()
        if normalized_policy not in {"ERROR", "DROP", "ALLOW"}:
            raise TransformationError("null_policy must be ERROR, DROP, or ALLOW")
        if required and normalized_policy == "DROP":
            transformed = transformed.dropna(subset=required)
        elif required and normalized_policy == "ERROR":
            null_condition = F.lit(False)
            for column_name in required:
                null_condition = null_condition | F.col(column_name).isNull()
            if transformed.filter(null_condition).limit(1).count():
                raise TransformationError(
                    "Required target columns contain null values: "
                    + ", ".join(required)
                )

        report = TransformationReport(
            pipeline_step_id=pipeline_step_id,
            mapped_columns=tuple(mapping.target_column for mapping in mappings),
            applied_rule_codes=tuple(applied_rules),
            defaulted_columns=tuple(defaulted),
            required_columns=tuple(required),
            null_policy=normalized_policy,
        )
        return TransformationResult(transformed, report)

    @staticmethod
    def _validate_unique_mappings(mappings: Sequence[ColumnMapping]) -> None:
        targets = [mapping.target_column.casefold() for mapping in mappings]
        orders = [mapping.mapping_order for mapping in mappings]
        if len(set(targets)) != len(targets):
            raise TransformationError("Column mappings contain duplicate targets")
        if len(set(orders)) != len(orders):
            raise TransformationError("Column mappings contain duplicate orders")

    def _apply_rule(self, dataframe: DataFrame, rule: TransformationRule) -> DataFrame:
        rule_type = rule.rule_type.strip().upper()
        expression = rule.rule_expression.strip()
        if not expression:
            raise TransformationError(f"Rule {rule.rule_code!r} has an empty expression")
        if rule_type == "FILTER":
            return dataframe.filter(F.expr(expression))
        if rule_type == "DERIVE":
            match = _DERIVE.match(expression)
            if not match:
                raise TransformationError(
                    f"DERIVE rule {rule.rule_code!r} must use 'column = expression'"
                )
            return dataframe.withColumn(match.group(1).strip(), F.expr(match.group(2)))
        if rule_type == "CAST":
            match = _CAST.match(expression)
            if not match:
                raise TransformationError(
                    f"CAST rule {rule.rule_code!r} must use 'column:type'"
                )
            column_name, data_type = match.group(1).strip(), match.group(2).strip()
            return dataframe.withColumn(column_name, F.col(column_name).cast(data_type))
        if rule_type == "RENAME":
            match = _RENAME.match(expression)
            if not match:
                raise TransformationError(
                    f"RENAME rule {rule.rule_code!r} must use 'old:new'"
                )
            return dataframe.withColumnRenamed(match.group(1).strip(), match.group(2).strip())
        if rule_type == "DEDUPLICATE":
            match = _DEDUPLICATE.match(expression)
            if not match:
                raise TransformationError(
                    f"DEDUPLICATE rule {rule.rule_code!r} must use "
                    "'key[,key] ORDER BY column [ASC|DESC]'"
                )
            keys = [key.strip() for key in match.group(1).split(",")]
            order_column = F.col(match.group(2))
            order_column = (
                order_column.asc()
                if (match.group(3) or "DESC").upper() == "ASC"
                else order_column.desc()
            )
            rank = F.row_number().over(Window.partitionBy(*keys).orderBy(order_column))
            return dataframe.withColumn("__etl_rank", rank).filter(
                F.col("__etl_rank") == 1
            ).drop("__etl_rank")
        raise TransformationError(
            f"Unsupported executable transformation rule type {rule.rule_type!r}"
        )
