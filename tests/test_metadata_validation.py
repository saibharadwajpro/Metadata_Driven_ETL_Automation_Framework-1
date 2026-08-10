from dataclasses import replace

import pytest

from etl_framework.metadata import MetadataValidationError, validate_metadata
from etl_framework.metadata.models import PipelineMetadata, PipelineStep


def test_validation_aggregates_multiple_errors() -> None:
    step = PipelineStep(
        pipeline_step_id=1,
        step_code="LOAD_CUSTOMER",
        step_name="Load Customer",
        step_type="LOAD",
        execution_order=1,
        source_dataset_id=None,
        target_dataset_id=None,
        load_type="INCREMENTAL",
        write_strategy="MERGE",
        watermark_column="ModifiedAt",
        retry_count=0,
        retry_interval_sec=60,
        timeout_sec=None,
    )
    metadata = PipelineMetadata(
        pipeline_id=1,
        pipeline_code="CUSTOMER",
        pipeline_name="Customer",
        environment_code="dev",
        metadata_version=1,
        steps=(step,),
        source_configurations=(),
        column_mappings=(),
        transformation_rules=(),
        validation_rules=(),
        watermark_configurations=(),
    )

    with pytest.raises(MetadataValidationError) as caught:
        validate_metadata(metadata)

    assert "requires a target dataset" in str(caught.value)
    assert "requires watermark configuration" in str(caught.value)


def test_metadata_records_are_immutable() -> None:
    step = PipelineStep(1, "A", "A", "CUSTOM", 1, None, None, None, None, None, 0, 0, None)

    changed = replace(step, step_name="B")

    assert step.step_name == "A"
    assert changed.step_name == "B"

