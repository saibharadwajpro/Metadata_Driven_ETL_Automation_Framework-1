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

## Design documentation

- [Architecture and execution workflow](docs/architecture.md)
- [Metadata configuration strategy](configuration/metadata-strategy.md)
- [Metadata repository and relationships](database/README.md)
- [Azure SQL metadata schema](database/schema.sql)
- [Sample metadata records](database/sample_metadata.sql)
