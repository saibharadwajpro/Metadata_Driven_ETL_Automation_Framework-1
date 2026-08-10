"""Metadata reader exceptions."""


class MetadataNotFoundError(LookupError):
    """Raised when an active pipeline configuration cannot be found."""


class MetadataValidationError(ValueError):
    """Raised when metadata is incomplete or internally inconsistent."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid ETL metadata: " + "; ".join(errors))

