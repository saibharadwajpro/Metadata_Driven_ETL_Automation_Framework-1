"""Reusable Spark source ingestion API."""

from .errors import SourceConfigurationError
from .models import ConnectivityResult, SourceType
from .reader import SparkSourceReader

__all__ = [
    "ConnectivityResult",
    "SourceConfigurationError",
    "SourceType",
    "SparkSourceReader",
]

