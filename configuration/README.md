# Configuration

1. Copy `.env.example` to `.env`.
2. Fill in the values for the target development environment.
3. Never commit `.env`, access tokens, client secrets, or database passwords.

## Authentication

For local development, Azure SDK clients use `DefaultAzureCredential`. Authenticate with Azure CLI when available, or supply the service-principal variables in `.env`. In Azure, prefer managed identity.

Databricks SDK reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Data Factory requires `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, and `AZURE_DATA_FACTORY_NAME`. SQL access uses `SQL_CONNECTION_STRING` and requires Microsoft ODBC Driver 18 for SQL Server.
