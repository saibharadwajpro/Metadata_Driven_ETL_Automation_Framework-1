/* Azure SQL / SQL Server metadata repository */
CREATE SCHEMA meta;
GO
CREATE SCHEMA audit;
GO

CREATE TABLE meta.system (
    system_id           bigint IDENTITY(1,1) PRIMARY KEY,
    system_code         varchar(100) NOT NULL UNIQUE,
    system_name         nvarchar(200) NOT NULL,
    system_type         varchar(40) NOT NULL,
    is_active           bit NOT NULL DEFAULT 1,
    created_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE meta.connection (
    connection_id       bigint IDENTITY(1,1) PRIMARY KEY,
    system_id           bigint NOT NULL REFERENCES meta.system(system_id),
    environment_code    varchar(20) NOT NULL,
    connection_name     nvarchar(200) NOT NULL,
    endpoint            nvarchar(1000) NULL,
    database_name       nvarchar(256) NULL,
    authentication_type varchar(40) NOT NULL,
    secret_reference    nvarchar(500) NULL,
    config_json         nvarchar(max) NULL,
    is_active           bit NOT NULL DEFAULT 1,
    created_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_connection UNIQUE (system_id, environment_code, connection_name),
    CONSTRAINT ck_connection_json CHECK (config_json IS NULL OR ISJSON(config_json) = 1)
);

CREATE TABLE meta.dataset (
    dataset_id          bigint IDENTITY(1,1) PRIMARY KEY,
    dataset_code        varchar(150) NOT NULL UNIQUE,
    connection_id       bigint NOT NULL REFERENCES meta.connection(connection_id),
    dataset_name        nvarchar(256) NOT NULL,
    object_path         nvarchar(1000) NOT NULL,
    data_format         varchar(40) NOT NULL,
    schema_evolution    varchar(20) NOT NULL DEFAULT 'STRICT',
    config_json         nvarchar(max) NULL,
    is_active           bit NOT NULL DEFAULT 1,
    created_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT ck_dataset_schema_evolution CHECK (schema_evolution IN ('STRICT','ADDITIVE','PERMISSIVE')),
    CONSTRAINT ck_dataset_json CHECK (config_json IS NULL OR ISJSON(config_json) = 1)
);

CREATE TABLE meta.dataset_column (
    dataset_column_id   bigint IDENTITY(1,1) PRIMARY KEY,
    dataset_id          bigint NOT NULL REFERENCES meta.dataset(dataset_id),
    column_name         nvarchar(256) NOT NULL,
    ordinal_position    int NOT NULL,
    data_type           varchar(100) NOT NULL,
    is_nullable         bit NOT NULL DEFAULT 1,
    is_business_key     bit NOT NULL DEFAULT 0,
    is_partition_column bit NOT NULL DEFAULT 0,
    sensitivity_class   varchar(40) NULL,
    default_expression  nvarchar(1000) NULL,
    CONSTRAINT uq_dataset_column_name UNIQUE (dataset_id, column_name),
    CONSTRAINT uq_dataset_column_ordinal UNIQUE (dataset_id, ordinal_position),
    CONSTRAINT ck_dataset_column_ordinal CHECK (ordinal_position > 0)
);

CREATE TABLE meta.pipeline (
    pipeline_id         bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_code       varchar(150) NOT NULL,
    pipeline_name       nvarchar(256) NOT NULL,
    environment_code    varchar(20) NOT NULL,
    metadata_version    int NOT NULL,
    description         nvarchar(1000) NULL,
    effective_from_utc  datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    effective_to_utc    datetime2(3) NULL,
    is_active           bit NOT NULL DEFAULT 0,
    created_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_pipeline_version UNIQUE (pipeline_code, environment_code, metadata_version),
    CONSTRAINT ck_pipeline_version CHECK (metadata_version > 0),
    CONSTRAINT ck_pipeline_effective_dates CHECK (effective_to_utc IS NULL OR effective_to_utc > effective_from_utc)
);

CREATE TABLE meta.pipeline_step (
    pipeline_step_id    bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_id         bigint NOT NULL REFERENCES meta.pipeline(pipeline_id),
    step_code           varchar(150) NOT NULL,
    step_name           nvarchar(256) NOT NULL,
    step_type           varchar(30) NOT NULL,
    execution_order     int NOT NULL,
    source_dataset_id   bigint NULL REFERENCES meta.dataset(dataset_id),
    target_dataset_id   bigint NULL REFERENCES meta.dataset(dataset_id),
    load_type           varchar(20) NULL,
    write_strategy      varchar(20) NULL,
    watermark_column    nvarchar(256) NULL,
    retry_count         smallint NOT NULL DEFAULT 0,
    retry_interval_sec  int NOT NULL DEFAULT 60,
    timeout_sec         int NULL,
    config_json         nvarchar(max) NULL,
    is_active           bit NOT NULL DEFAULT 1,
    CONSTRAINT uq_pipeline_step_code UNIQUE (pipeline_id, step_code),
    CONSTRAINT uq_pipeline_step_order UNIQUE (pipeline_id, execution_order),
    CONSTRAINT uq_pipeline_step_identity UNIQUE (pipeline_step_id, pipeline_id),
    CONSTRAINT ck_step_type CHECK (step_type IN ('INGEST','TRANSFORM','VALIDATE','LOAD','CUSTOM')),
    CONSTRAINT ck_step_load_type CHECK (load_type IS NULL OR load_type IN ('FULL','INCREMENTAL','CDC')),
    CONSTRAINT ck_step_write_strategy CHECK (write_strategy IS NULL OR write_strategy IN ('APPEND','OVERWRITE','MERGE','SCD1','SCD2')),
    CONSTRAINT ck_step_retry CHECK (retry_count >= 0 AND retry_interval_sec >= 0),
    CONSTRAINT ck_step_timeout CHECK (timeout_sec IS NULL OR timeout_sec > 0),
    CONSTRAINT ck_step_json CHECK (config_json IS NULL OR ISJSON(config_json) = 1)
);

CREATE TABLE meta.step_dependency (
    pipeline_id                 bigint NOT NULL REFERENCES meta.pipeline(pipeline_id),
    pipeline_step_id            bigint NOT NULL,
    depends_on_pipeline_step_id bigint NOT NULL,
    dependency_condition        varchar(20) NOT NULL DEFAULT 'SUCCEEDED',
    PRIMARY KEY (pipeline_step_id, depends_on_pipeline_step_id),
    CONSTRAINT fk_dependency_step FOREIGN KEY (pipeline_step_id, pipeline_id)
        REFERENCES meta.pipeline_step(pipeline_step_id, pipeline_id),
    CONSTRAINT fk_dependency_prerequisite FOREIGN KEY (depends_on_pipeline_step_id, pipeline_id)
        REFERENCES meta.pipeline_step(pipeline_step_id, pipeline_id),
    CONSTRAINT ck_dependency_self CHECK (pipeline_step_id <> depends_on_pipeline_step_id),
    CONSTRAINT ck_dependency_condition CHECK (dependency_condition IN ('SUCCEEDED','COMPLETED'))
);

CREATE TABLE meta.pipeline_parameter (
    pipeline_parameter_id bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_id         bigint NOT NULL REFERENCES meta.pipeline(pipeline_id),
    parameter_name      varchar(150) NOT NULL,
    data_type           varchar(30) NOT NULL,
    default_value       nvarchar(2000) NULL,
    is_required         bit NOT NULL DEFAULT 0,
    allow_runtime_override bit NOT NULL DEFAULT 0,
    CONSTRAINT uq_pipeline_parameter UNIQUE (pipeline_id, parameter_name),
    CONSTRAINT ck_parameter_type CHECK (data_type IN ('STRING','INTEGER','DECIMAL','BOOLEAN','DATE','TIMESTAMP','JSON'))
);

CREATE TABLE meta.column_mapping (
    column_mapping_id   bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_step_id    bigint NOT NULL REFERENCES meta.pipeline_step(pipeline_step_id),
    mapping_order       int NOT NULL,
    source_column       nvarchar(256) NULL,
    target_column       nvarchar(256) NOT NULL,
    transform_expression nvarchar(max) NULL,
    target_data_type    varchar(100) NULL,
    is_active           bit NOT NULL DEFAULT 1,
    CONSTRAINT uq_column_mapping_order UNIQUE (pipeline_step_id, mapping_order),
    CONSTRAINT uq_column_mapping_target UNIQUE (pipeline_step_id, target_column),
    CONSTRAINT ck_mapping_order CHECK (mapping_order > 0)
);

CREATE TABLE meta.validation_rule (
    validation_rule_id  bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_step_id    bigint NOT NULL REFERENCES meta.pipeline_step(pipeline_step_id),
    rule_code           varchar(150) NOT NULL,
    rule_type           varchar(40) NOT NULL,
    rule_expression     nvarchar(max) NOT NULL,
    severity            varchar(10) NOT NULL DEFAULT 'ERROR',
    threshold_value     decimal(18,6) NULL,
    is_active           bit NOT NULL DEFAULT 1,
    CONSTRAINT uq_validation_rule UNIQUE (pipeline_step_id, rule_code),
    CONSTRAINT ck_rule_type CHECK (rule_type IN ('NOT_NULL','UNIQUE','RANGE','REGEX','REFERENTIAL','ROW_COUNT','CUSTOM_SQL')),
    CONSTRAINT ck_rule_severity CHECK (severity IN ('INFO','WARNING','ERROR')),
    CONSTRAINT ck_rule_threshold CHECK (threshold_value IS NULL OR threshold_value >= 0)
);

CREATE TABLE meta.pipeline_schedule (
    pipeline_schedule_id bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_id         bigint NOT NULL REFERENCES meta.pipeline(pipeline_id),
    schedule_name       nvarchar(200) NOT NULL,
    schedule_type       varchar(20) NOT NULL,
    schedule_expression nvarchar(500) NOT NULL,
    time_zone           varchar(100) NOT NULL DEFAULT 'UTC',
    is_active           bit NOT NULL DEFAULT 1,
    CONSTRAINT uq_pipeline_schedule UNIQUE (pipeline_id, schedule_name),
    CONSTRAINT ck_schedule_type CHECK (schedule_type IN ('CRON','TUMBLING_WINDOW','EVENT'))
);

CREATE TABLE audit.etl_run (
    etl_run_id          uniqueidentifier NOT NULL DEFAULT NEWSEQUENTIALID() PRIMARY KEY,
    pipeline_id         bigint NOT NULL REFERENCES meta.pipeline(pipeline_id),
    parent_etl_run_id   uniqueidentifier NULL REFERENCES audit.etl_run(etl_run_id),
    trigger_type        varchar(30) NOT NULL,
    orchestrator_run_id nvarchar(200) NULL,
    status              varchar(20) NOT NULL DEFAULT 'QUEUED',
    runtime_parameters_json nvarchar(max) NULL,
    started_at_utc      datetime2(3) NULL,
    ended_at_utc        datetime2(3) NULL,
    error_code          varchar(100) NULL,
    error_message       nvarchar(4000) NULL,
    created_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT ck_run_status CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','PARTIAL')),
    CONSTRAINT ck_run_parameters_json CHECK (runtime_parameters_json IS NULL OR ISJSON(runtime_parameters_json) = 1),
    CONSTRAINT ck_run_dates CHECK (ended_at_utc IS NULL OR started_at_utc IS NULL OR ended_at_utc >= started_at_utc)
);

CREATE TABLE audit.etl_step_run (
    etl_step_run_id     uniqueidentifier NOT NULL DEFAULT NEWSEQUENTIALID() PRIMARY KEY,
    etl_run_id          uniqueidentifier NOT NULL REFERENCES audit.etl_run(etl_run_id),
    pipeline_step_id    bigint NOT NULL REFERENCES meta.pipeline_step(pipeline_step_id),
    attempt_number      smallint NOT NULL DEFAULT 1,
    status              varchar(20) NOT NULL DEFAULT 'QUEUED',
    started_at_utc      datetime2(3) NULL,
    ended_at_utc        datetime2(3) NULL,
    rows_read           bigint NULL,
    rows_written        bigint NULL,
    rows_rejected       bigint NULL,
    metrics_json        nvarchar(max) NULL,
    error_code          varchar(100) NULL,
    error_message       nvarchar(4000) NULL,
    CONSTRAINT uq_step_run_attempt UNIQUE (etl_run_id, pipeline_step_id, attempt_number),
    CONSTRAINT ck_step_run_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_step_run_status CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
    CONSTRAINT ck_step_run_counts CHECK ((rows_read IS NULL OR rows_read >= 0) AND (rows_written IS NULL OR rows_written >= 0) AND (rows_rejected IS NULL OR rows_rejected >= 0)),
    CONSTRAINT ck_step_run_metrics_json CHECK (metrics_json IS NULL OR ISJSON(metrics_json) = 1),
    CONSTRAINT ck_step_run_dates CHECK (ended_at_utc IS NULL OR started_at_utc IS NULL OR ended_at_utc >= started_at_utc)
);

CREATE TABLE audit.quality_result (
    quality_result_id   bigint IDENTITY(1,1) PRIMARY KEY,
    etl_step_run_id     uniqueidentifier NOT NULL REFERENCES audit.etl_step_run(etl_step_run_id),
    validation_rule_id  bigint NOT NULL REFERENCES meta.validation_rule(validation_rule_id),
    passed              bit NOT NULL,
    observed_value      nvarchar(1000) NULL,
    failed_row_count    bigint NULL,
    evaluated_at_utc    datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_quality_result UNIQUE (etl_step_run_id, validation_rule_id),
    CONSTRAINT ck_quality_failed_count CHECK (failed_row_count IS NULL OR failed_row_count >= 0)
);

CREATE TABLE audit.watermark (
    watermark_id        bigint IDENTITY(1,1) PRIMARY KEY,
    pipeline_step_id    bigint NOT NULL REFERENCES meta.pipeline_step(pipeline_step_id),
    watermark_name      varchar(150) NOT NULL,
    watermark_value     nvarchar(1000) NOT NULL,
    value_data_type     varchar(30) NOT NULL,
    source_etl_run_id   uniqueidentifier NOT NULL REFERENCES audit.etl_run(etl_run_id),
    updated_at_utc      datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    row_version         rowversion NOT NULL,
    CONSTRAINT uq_watermark UNIQUE (pipeline_step_id, watermark_name),
    CONSTRAINT ck_watermark_type CHECK (value_data_type IN ('INTEGER','DECIMAL','DATE','TIMESTAMP','STRING'))
);

CREATE INDEX ix_pipeline_active
    ON meta.pipeline (pipeline_code, environment_code, is_active, effective_from_utc);
CREATE UNIQUE INDEX ux_pipeline_one_active_version
    ON meta.pipeline (pipeline_code, environment_code)
    WHERE is_active = 1;
CREATE INDEX ix_step_pipeline_active
    ON meta.pipeline_step (pipeline_id, is_active, execution_order);
CREATE INDEX ix_run_pipeline_status
    ON audit.etl_run (pipeline_id, status, created_at_utc DESC);
CREATE INDEX ix_step_run_run_status
    ON audit.etl_step_run (etl_run_id, status);
GO
