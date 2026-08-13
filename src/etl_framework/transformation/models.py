"""Transformation execution results."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class TransformationReport:
    pipeline_step_id: int
    mapped_columns: tuple[str, ...]
    applied_rule_codes: tuple[str, ...]
    defaulted_columns: tuple[str, ...]
    required_columns: tuple[str, ...]
    null_policy: str


@dataclass(frozen=True)
class TransformationResult:
    dataframe: "DataFrame"
    report: TransformationReport

