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
