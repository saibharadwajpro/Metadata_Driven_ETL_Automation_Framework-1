"""Public API for reading and validating ETL metadata."""

from .errors import MetadataNotFoundError, MetadataValidationError
from .models import (
    ColumnMapping,
    PipelineMetadata,
    PipelineStep,
    SourceConfiguration,
    TransformationRule,
    ValidationRule,
    WatermarkConfiguration,
)
from .reader import MetadataReader, pyodbc_connection_factory
from .validation import validate_metadata

__all__ = [
    "ColumnMapping",
    "MetadataNotFoundError",
    "MetadataReader",
    "MetadataValidationError",
    "PipelineMetadata",
    "PipelineStep",
    "SourceConfiguration",
    "TransformationRule",
    "ValidationRule",
    "WatermarkConfiguration",
    "pyodbc_connection_factory",
    "validate_metadata",
]

