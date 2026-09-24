"""Reusable Delta Lake write utilities."""

from pyspark.sql import DataFrame


def overwrite_delta_table(
    df: DataFrame,
    table_name: str,
    overwrite_schema: bool = True,
) -> None:
    """
    Overwrite a managed Delta table with a DataFrame.

    Parameters
    ----------
    df:
        Spark DataFrame to persist.

    table_name:
        Fully qualified Unity Catalog table name.

    overwrite_schema:
        Whether Delta should replace the existing table schema.
    """

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
    )

    if overwrite_schema:
        writer = writer.option(
            "overwriteSchema",
            "true",
        )

    writer.saveAsTable(table_name)