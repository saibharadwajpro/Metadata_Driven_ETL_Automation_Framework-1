# Metadata-Driven ETL Automation Framework

## Project structure

- `ingestion/` - source ingestion components
- `transformation/` - data transformation components
- `validation/` - data-quality validation components
- `loading/` - destination loading components
- `logging/` - logging configuration and utilities
- `configuration/` - environment and service configuration guidance

## Development setup

Prerequisites:

- Python 3.11 or newer
- Java 17
- Microsoft ODBC Driver 18 for SQL Server (for Azure SQL connectivity)
- Access to the target Azure subscription, Databricks workspace, Data Factory, and SQL database

Create and activate the environment on Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Fill in `.env` using the instructions in `configuration/README.md`. Secrets and local environments are ignored by Git.

## Verify

Confirm the local runtimes and installed libraries:

```powershell
python --version
java -version
python -c "import pyspark, databricks.sdk, azure.identity, azure.mgmt.datafactory, pyodbc, dotenv; print('Dependencies OK')"
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').appName('environment-check').getOrCreate(); print(s.version); s.stop()"
```

Azure connectivity can only be verified after valid development credentials and target resource identifiers are placed in the local `.env` file.

## Metadata reader

The `etl_framework.metadata` package reads an active pipeline and all related source configurations, column mappings, transformation rules, validation rules, and watermark settings. It validates the complete manifest before returning it for execution.

```python
import os

from dotenv import load_dotenv
from etl_framework.metadata import MetadataReader, pyodbc_connection_factory

load_dotenv()
reader = MetadataReader(
    pyodbc_connection_factory(os.environ["SQL_CONNECTION_STRING"])
)
metadata = reader.load_pipeline_metadata("CUSTOMER_INCREMENTAL", "dev")
```

The connection string is read at runtime and is never logged or stored in metadata.

## Reusable source ingestion

`SparkSourceReader` loads CSV, JSON, Parquet, and SQL/JDBC sources into Spark DataFrames from `SourceConfiguration` metadata. File-reader defaults can be overridden in metadata. SQL credentials should be resolved from Key Vault or managed identity and supplied through `runtime_options`, never stored in metadata.

```python
from etl_framework.ingestion import SparkSourceReader

source_reader = SparkSourceReader(spark)
connectivity = source_reader.validate_connectivity(source_configuration)
if not connectivity.success:
    raise RuntimeError(connectivity.message)

dataframe = source_reader.read(source_configuration)
```

See `configuration/source-ingestion.example.json` for all four source configurations.

## Schema validation

The metadata reader loads the expected schema from `meta.dataset_column`. `SchemaValidator` compares it with a Spark DataFrame schema without scanning data and generates a structured report containing missing columns, unexpected columns, name/case differences, data-type mismatches, and nullability mismatches.

```python
from etl_framework.validation import SchemaValidator

expected = tuple(c for c in metadata.dataset_columns if c.dataset_id == source_dataset_id)
report = SchemaValidator().validate_dataframe(dataframe, expected)
if not report.is_valid:
    print(report.to_dict())
```

## Metadata-driven transformations

`TransformationEngine` reads the existing column mappings, transformation rules, and target-column metadata to produce a load-ready Spark DataFrame. It supports dynamic expressions and casts, target defaults, required-column null policies (`ERROR`, `DROP`, or `ALLOW`), and ordered `FILTER`, `DERIVE`, `CAST`, `RENAME`, and `DEDUPLICATE` business rules.

```python
from etl_framework.transformation import TransformationEngine

result = TransformationEngine().transform(
    dataframe,
    pipeline_step_id=step.pipeline_step_id,
    column_mappings=metadata.column_mappings,
    transformation_rules=metadata.transformation_rules,
    target_columns=tuple(
        column for column in metadata.dataset_columns
        if column.dataset_id == step.target_dataset_id
    ),
)
load_ready_dataframe = result.dataframe
```

Transformation expressions are trusted configuration and should pass metadata review before activation. Rules requiring another DataFrame, such as joins and aggregations, are rejected by this single-input engine and should be executed by an explicitly configured multi-input component.

## Record validation and rejection

`DataValidator` applies expected-column metadata and active validation rules to Spark records. Non-nullable columns become mandatory checks, declared data types are verified with safe casts, business-key or `UNIQUE` rules identify duplicates, and boolean `NOT_NULL`, `RANGE`, `REGEX`, `REFERENTIAL`, and `CUSTOM_SQL` expressions are evaluated as pass conditions.

```python
from etl_framework.validation import DataValidator

result = DataValidator().validate(
    load_ready_dataframe,
    pipeline_step_id=step.pipeline_step_id,
    expected_columns=target_columns,
    validation_rules=metadata.validation_rules,
)
accepted_dataframe = result.accepted
rejected_dataframe = result.rejected
```

Rejected rows retain their source fields and include `__validation_errors` and `__validation_warnings`. Warning and informational failures are reported but do not reject a row. The report contains total, accepted, rejected, and rule-level failure counts.

## Design documentation

- [Architecture and execution workflow](docs/architecture.md)
- [Metadata configuration strategy](configuration/metadata-strategy.md)
- [Metadata repository and relationships](database/README.md)
- [Azure SQL metadata schema](database/schema.sql)
- [Sample metadata records](database/sample_metadata.sql)
