from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.metadata.models import DatasetColumn
from etl_framework.validation import SchemaValidator


def expected(name: str, data_type: str, *, position: int, nullable: bool = True) -> DatasetColumn:
    return DatasetColumn(position, 10, name, position, data_type, nullable, False, False, None, None)


def test_valid_schema_generates_success_report() -> None:
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
    ])
    report = SchemaValidator().validate_schema(schema, [
        expected("customer_id", "INTEGER", position=1, nullable=False),
        expected("customer_name", "VARCHAR(200)", position=2),
    ])

    assert report.is_valid
    assert report.issues == ()
    assert report.to_dict()["dataset_id"] == 10


def test_report_identifies_names_types_missing_and_unexpected_columns() -> None:
    schema = StructType([
        StructField("Customer_ID", StringType(), True),
        StructField("extra_column", StringType(), True),
    ])
    report = SchemaValidator().validate_schema(schema, [
        expected("customer_id", "INTEGER", position=1, nullable=False),
        expected("email", "STRING", position=2),
    ])

    categories = {issue.category for issue in report.issues}
    assert not report.is_valid
    assert categories == {"COLUMN_NAME_CASE", "TYPE_MISMATCH", "NULLABILITY_MISMATCH", "MISSING_COLUMN", "UNEXPECTED_COLUMN"}
    assert report.missing_columns == ("email",)
    assert report.unexpected_columns == ("extra_column",)


def test_can_allow_unexpected_columns() -> None:
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("audit_id", StringType(), True),
    ])
    report = SchemaValidator().validate_schema(
        schema, [expected("id", "INT", position=1, nullable=False)],
        allow_unexpected_columns=True,
    )

    assert report.is_valid

