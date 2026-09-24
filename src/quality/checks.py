"""Reusable Spark data-quality checks."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_missing_value_flag(
    df: DataFrame,
    column_name: str,
    flag_name: str,
) -> DataFrame:
    """Flag records where a required value is null or blank."""

    return df.withColumn(
        flag_name,
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name).cast("string")) == ""),
    )


def add_negative_value_flag(
    df: DataFrame,
    column_name: str,
    flag_name: str,
) -> DataFrame:
    """Flag records containing a negative numeric value."""

    return df.withColumn(
        flag_name,
        F.col(column_name) < 0,
    )


def add_invalid_date_order_flag(
    df: DataFrame,
    start_date_column: str,
    end_date_column: str,
    flag_name: str,
) -> DataFrame:
    """Flag records where an end date occurs before its start date."""

    return df.withColumn(
        flag_name,
        F.col(end_date_column) < F.col(start_date_column),
    )


def duplicate_key_count(
    df: DataFrame,
    key_columns: list[str],
) -> int:
    """Return the number of duplicated key combinations."""

    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )