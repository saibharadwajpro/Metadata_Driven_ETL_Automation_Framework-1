"""Models shared by source readers."""

from dataclasses import dataclass
from enum import StrEnum


class SourceType(StrEnum):
    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "PARQUET"
    SQL = "SQL"


@dataclass(frozen=True)
class ConnectivityResult:
    source_type: SourceType | None
    success: bool
    message: str
