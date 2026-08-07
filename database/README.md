# Metadata Repository

## Files

- `schema.sql` creates the Azure SQL / SQL Server metadata and audit schemas.
- `sample_metadata.sql` inserts a complete, repeatable customer-ingestion example without credentials.

Run `schema.sql` once against an empty development database, then run `sample_metadata.sql`. The seed script is idempotent and can be rerun safely.

## Requested tables

| Requirement | Table | Purpose |
|---|---|---|
| Source Configuration | `meta.source_configuration` | Extraction mode, query/filter, batching, landing path, and connector options |
| Column Mapping | `meta.column_mapping` | Source-to-target column definitions and expressions |
| Transformation Rules | `meta.transformation_rule` | Ordered filters, derivations, casts, joins, and other transformations |
| Validation Rules | `meta.validation_rule` | Data-quality expressions, thresholds, and severity |
| Watermark Configuration | `meta.watermark_configuration` | Incremental boundary definition and initial value |
| Execution Log | `audit.execution_log` | Structured operational events for a run or step attempt |
| Error Log | `audit.error_log` | Detailed, retry-aware failure records linked to execution events |

`audit.watermark` stores the current runtime watermark value; `meta.watermark_configuration` defines how that value is calculated. Existing `audit.etl_run` and `audit.etl_step_run` tables remain the authoritative execution summaries.

Credentials are deliberately absent. Connection metadata contains only a Key Vault reference or managed-identity configuration.
