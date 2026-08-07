/* Repeatable, non-secret sample metadata for a customer ingestion pipeline. */
SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF NOT EXISTS (SELECT 1 FROM meta.system WHERE system_code = 'SAMPLE_AZURE_SQL')
    INSERT INTO meta.system (system_code, system_name, system_type)
    VALUES ('SAMPLE_AZURE_SQL', N'Sample Azure SQL Source', 'AZURE_SQL');

DECLARE @system_id bigint = (SELECT system_id FROM meta.system WHERE system_code = 'SAMPLE_AZURE_SQL');

IF NOT EXISTS (SELECT 1 FROM meta.connection WHERE system_id = @system_id AND environment_code = 'dev' AND connection_name = N'sample-customer-source')
    INSERT INTO meta.connection (system_id, environment_code, connection_name, endpoint, database_name, authentication_type, secret_reference, config_json)
    VALUES (@system_id, 'dev', N'sample-customer-source', N'sample-server.database.windows.net', N'SampleSales', 'KEY_VAULT', N'kv://sample-vault/secrets/sample-sql-connection', N'{"encrypt":true,"trustServerCertificate":false}');

DECLARE @connection_id bigint = (SELECT connection_id FROM meta.connection WHERE system_id = @system_id AND environment_code = 'dev' AND connection_name = N'sample-customer-source');

IF NOT EXISTS (SELECT 1 FROM meta.dataset WHERE dataset_code = 'SRC_CUSTOMER')
    INSERT INTO meta.dataset (dataset_code, connection_id, dataset_name, object_path, data_format, schema_evolution, config_json)
    VALUES ('SRC_CUSTOMER', @connection_id, N'Customer Source', N'dbo.Customer', 'TABLE', 'ADDITIVE', N'{"partitionColumn":"CustomerId"}');

IF NOT EXISTS (SELECT 1 FROM meta.dataset WHERE dataset_code = 'TGT_CUSTOMER_CURATED')
    INSERT INTO meta.dataset (dataset_code, connection_id, dataset_name, object_path, data_format, schema_evolution, config_json)
    VALUES ('TGT_CUSTOMER_CURATED', @connection_id, N'Customer Curated', N'curated/customer', 'DELTA', 'ADDITIVE', N'{"mergeSchema":true}');

DECLARE @source_dataset_id bigint = (SELECT dataset_id FROM meta.dataset WHERE dataset_code = 'SRC_CUSTOMER');
DECLARE @target_dataset_id bigint = (SELECT dataset_id FROM meta.dataset WHERE dataset_code = 'TGT_CUSTOMER_CURATED');

IF NOT EXISTS (SELECT 1 FROM meta.source_configuration WHERE dataset_id = @source_dataset_id)
    INSERT INTO meta.source_configuration (dataset_id, source_query, load_type, batch_size, fetch_size, source_filter, landing_path, file_format, options_json)
    VALUES (@source_dataset_id, N'SELECT CustomerId, CustomerName, Email, ModifiedAt FROM dbo.Customer', 'INCREMENTAL', 10000, 5000, N'IsDeleted = 0', N'abfss://raw@samplelake.dfs.core.windows.net/customer/', 'PARQUET', N'{"compression":"snappy"}');

IF NOT EXISTS (SELECT 1 FROM meta.pipeline WHERE pipeline_code = 'CUSTOMER_INCREMENTAL' AND environment_code = 'dev' AND metadata_version = 1)
    INSERT INTO meta.pipeline (pipeline_code, pipeline_name, environment_code, metadata_version, description, is_active)
    VALUES ('CUSTOMER_INCREMENTAL', N'Customer Incremental Load', 'dev', 1, N'Sample metadata-driven customer ingestion and transformation.', 1);

DECLARE @pipeline_id bigint = (SELECT pipeline_id FROM meta.pipeline WHERE pipeline_code = 'CUSTOMER_INCREMENTAL' AND environment_code = 'dev' AND metadata_version = 1);

IF NOT EXISTS (SELECT 1 FROM meta.pipeline_step WHERE pipeline_id = @pipeline_id AND step_code = 'INGEST_CUSTOMER')
    INSERT INTO meta.pipeline_step (pipeline_id, step_code, step_name, step_type, execution_order, source_dataset_id, target_dataset_id, load_type, write_strategy, watermark_column, retry_count)
    VALUES (@pipeline_id, 'INGEST_CUSTOMER', N'Ingest Customer', 'INGEST', 1, @source_dataset_id, @target_dataset_id, 'INCREMENTAL', 'MERGE', N'ModifiedAt', 2);

DECLARE @step_id bigint = (SELECT pipeline_step_id FROM meta.pipeline_step WHERE pipeline_id = @pipeline_id AND step_code = 'INGEST_CUSTOMER');

IF NOT EXISTS (SELECT 1 FROM meta.column_mapping WHERE pipeline_step_id = @step_id)
    INSERT INTO meta.column_mapping (pipeline_step_id, mapping_order, source_column, target_column, transform_expression, target_data_type)
    VALUES
        (@step_id, 1, N'CustomerId', N'customer_id', N'CAST(CustomerId AS BIGINT)', 'BIGINT'),
        (@step_id, 2, N'CustomerName', N'customer_name', N'TRIM(CustomerName)', 'STRING'),
        (@step_id, 3, N'Email', N'email', N'LOWER(TRIM(Email))', 'STRING'),
        (@step_id, 4, N'ModifiedAt', N'modified_at', N'CAST(ModifiedAt AS TIMESTAMP)', 'TIMESTAMP');

IF NOT EXISTS (SELECT 1 FROM meta.transformation_rule WHERE pipeline_step_id = @step_id)
    INSERT INTO meta.transformation_rule (pipeline_step_id, rule_code, rule_order, rule_type, rule_expression, rule_description)
    VALUES
        (@step_id, 'ACTIVE_CUSTOMERS', 1, 'FILTER', N'CustomerId IS NOT NULL', N'Remove rows without a customer identifier.'),
        (@step_id, 'DEDUP_CUSTOMERS', 2, 'DEDUPLICATE', N'CustomerId ORDER BY ModifiedAt DESC', N'Keep the most recent customer record.');

IF NOT EXISTS (SELECT 1 FROM meta.validation_rule WHERE pipeline_step_id = @step_id)
    INSERT INTO meta.validation_rule (pipeline_step_id, rule_code, rule_type, rule_expression, severity, threshold_value)
    VALUES
        (@step_id, 'CUSTOMER_ID_REQUIRED', 'NOT_NULL', N'customer_id IS NOT NULL', 'ERROR', 0),
        (@step_id, 'CUSTOMER_ID_UNIQUE', 'UNIQUE', N'customer_id', 'ERROR', 0),
        (@step_id, 'EMAIL_FORMAT', 'REGEX', N'email RLIKE ''^[^@]+@[^@]+\.[^@]+$''', 'WARNING', 0.05);

IF NOT EXISTS (SELECT 1 FROM meta.watermark_configuration WHERE pipeline_step_id = @step_id)
    INSERT INTO meta.watermark_configuration (pipeline_step_id, watermark_name, watermark_column, value_data_type, initial_value, comparison_operator, lookback_value)
    VALUES (@step_id, 'CUSTOMER_MODIFIED_AT', N'ModifiedAt', 'TIMESTAMP', N'1900-01-01T00:00:00Z', '>', 5);

DECLARE @sample_run_id uniqueidentifier = '11111111-1111-1111-1111-111111111111';
DECLARE @sample_step_run_id uniqueidentifier = '22222222-2222-2222-2222-222222222222';

IF NOT EXISTS (SELECT 1 FROM audit.etl_run WHERE etl_run_id = @sample_run_id)
    INSERT INTO audit.etl_run (etl_run_id, pipeline_id, trigger_type, orchestrator_run_id, status, runtime_parameters_json, started_at_utc, ended_at_utc)
    VALUES (@sample_run_id, @pipeline_id, 'MANUAL', N'sample-adf-run-001', 'SUCCEEDED', N'{"businessDate":"2026-08-07"}', '2026-08-07T12:00:00', '2026-08-07T12:03:00');

IF NOT EXISTS (SELECT 1 FROM audit.etl_step_run WHERE etl_step_run_id = @sample_step_run_id)
    INSERT INTO audit.etl_step_run (etl_step_run_id, etl_run_id, pipeline_step_id, attempt_number, status, started_at_utc, ended_at_utc, rows_read, rows_written, rows_rejected, metrics_json)
    VALUES (@sample_step_run_id, @sample_run_id, @step_id, 1, 'SUCCEEDED', '2026-08-07T12:00:05', '2026-08-07T12:02:55', 1000, 995, 5, N'{"durationSeconds":170,"partitions":4}');

IF NOT EXISTS (SELECT 1 FROM audit.execution_log WHERE etl_run_id = @sample_run_id AND event_type = 'STEP_COMPLETED')
    INSERT INTO audit.execution_log (etl_run_id, etl_step_run_id, log_level, event_type, log_message, details_json, logged_at_utc)
    VALUES (@sample_run_id, @sample_step_run_id, 'INFO', 'STEP_COMPLETED', N'Customer ingestion completed successfully.', N'{"rowsRead":1000,"rowsWritten":995,"rowsRejected":5}', '2026-08-07T12:02:55');

IF NOT EXISTS (SELECT 1 FROM audit.error_log WHERE etl_run_id = @sample_run_id AND error_code = 'SAMPLE_REJECTED_ROWS')
    INSERT INTO audit.error_log (etl_run_id, etl_step_run_id, error_code, error_type, error_message, source_component, is_retriable, occurred_at_utc)
    VALUES (@sample_run_id, @sample_step_run_id, 'SAMPLE_REJECTED_ROWS', N'DataQualityWarning', N'Five sample rows failed the email-format warning rule.', N'validation-engine', 0, '2026-08-07T12:02:50');

COMMIT TRANSACTION;
