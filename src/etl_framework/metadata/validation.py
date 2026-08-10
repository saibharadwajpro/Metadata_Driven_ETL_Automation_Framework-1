"""Pre-execution metadata validation."""

from collections import Counter, defaultdict

from .errors import MetadataValidationError
from .models import PipelineMetadata


def _duplicates(values: list[object]) -> list[object]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


def validate_metadata(metadata: PipelineMetadata) -> None:
    """Raise one aggregated error containing every detected metadata problem."""
    errors: list[str] = []
    if not metadata.steps:
        errors.append("pipeline has no active steps")
        raise MetadataValidationError(errors)

    step_ids = {step.pipeline_step_id for step in metadata.steps}
    step_orders = [step.execution_order for step in metadata.steps]
    duplicate_orders = _duplicates(step_orders)
    if duplicate_orders:
        errors.append(f"duplicate step execution orders: {duplicate_orders}")

    source_config_by_dataset = {
        config.dataset_id: config for config in metadata.source_configurations
    }
    watermark_by_step = defaultdict(list)
    for config in metadata.watermark_configurations:
        watermark_by_step[config.pipeline_step_id].append(config)

    for step in metadata.steps:
        label = f"step {step.step_code!r}"
        if step.step_type == "INGEST":
            if step.source_dataset_id is None:
                errors.append(f"{label} requires a source dataset")
            elif step.source_dataset_id not in source_config_by_dataset:
                errors.append(f"{label} has no active source configuration")
        if step.step_type == "LOAD" and step.target_dataset_id is None:
            errors.append(f"{label} requires a target dataset")
        if step.load_type == "INCREMENTAL":
            configs = watermark_by_step[step.pipeline_step_id]
            if not configs:
                errors.append(f"{label} requires watermark configuration")
            elif step.watermark_column and all(
                config.watermark_column.casefold() != step.watermark_column.casefold()
                for config in configs
            ):
                errors.append(f"{label} watermark column does not match its configuration")

    mappings_by_step = defaultdict(list)
    for mapping in metadata.column_mappings:
        mappings_by_step[mapping.pipeline_step_id].append(mapping)
        if mapping.pipeline_step_id not in step_ids:
            errors.append(f"column mapping {mapping.column_mapping_id} references an inactive step")
    for step_id, mappings in mappings_by_step.items():
        duplicate_mapping_orders = _duplicates([item.mapping_order for item in mappings])
        duplicate_targets = _duplicates(
            [item.target_column.casefold() for item in mappings]
        )
        if duplicate_mapping_orders:
            errors.append(f"step {step_id} has duplicate mapping orders: {duplicate_mapping_orders}")
        if duplicate_targets:
            errors.append(f"step {step_id} has duplicate mapping targets: {duplicate_targets}")

    transform_by_step = defaultdict(list)
    for rule in metadata.transformation_rules:
        transform_by_step[rule.pipeline_step_id].append(rule)
        if not rule.rule_expression.strip():
            errors.append(f"transformation rule {rule.rule_code!r} has an empty expression")
    for step_id, rules in transform_by_step.items():
        duplicate_rule_orders = _duplicates([rule.rule_order for rule in rules])
        if duplicate_rule_orders:
            errors.append(f"step {step_id} has duplicate transformation orders: {duplicate_rule_orders}")

    for rule in metadata.validation_rules:
        if not rule.rule_expression.strip():
            errors.append(f"validation rule {rule.rule_code!r} has an empty expression")
        if rule.threshold_value is not None and rule.threshold_value < 0:
            errors.append(f"validation rule {rule.rule_code!r} has a negative threshold")

    if errors:
        raise MetadataValidationError(errors)

