"""Incremental loading and merge/upsert components."""

from .incremental import (
    DataFrameUpsertTarget,
    IncrementalLoadCoordinator,
    IncrementalLoadError,
    IncrementalLoadResult,
    WatermarkRepository,
    WatermarkState,
)

__all__ = [
    "DataFrameUpsertTarget",
    "IncrementalLoadCoordinator",
    "IncrementalLoadError",
    "IncrementalLoadResult",
    "WatermarkRepository",
    "WatermarkState",
]

