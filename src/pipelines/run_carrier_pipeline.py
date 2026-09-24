"""Databricks execution entry point for the Carrier claims pipeline."""

from pyspark.sql import SparkSession

from config.project_config import (
    CARRIER_RAW_PATH,
    bronze_table,
    gold_table,
    silver_table,
)
from src.pipelines.bronze.carrier import (
    build_carrier_bronze,
)
from src.pipelines.gold.carrier import (
    build_carrier_yearly_summary,
)
from src.pipelines.silver.carrier import (
    build_carrier_claims_silver,
    build_carrier_service_lines,
)
from src.utils.delta import overwrite_delta_table


def run_carrier_pipeline(
    spark: SparkSession,
) -> None:
    """
    Execute the Carrier Bronze → Silver → Gold pipeline.
    """

    # -------------------------
    # Bronze
    # -------------------------

    bronze_df = build_carrier_bronze(
        spark=spark,
        source_paths=[CARRIER_RAW_PATH],
    )

    overwrite_delta_table(
        df=bronze_df,
        table_name=bronze_table(
            "carrier_claims"
        ),
    )

    # -------------------------
    # Silver — Claims
    # -------------------------

    silver_claims_df = build_carrier_claims_silver(
        bronze_df
    )

    overwrite_delta_table(
        df=silver_claims_df,
        table_name=silver_table(
            "carrier_claims"
        ),
    )

    # -------------------------
    # Silver — Service Lines
    # -------------------------

    service_lines_df = build_carrier_service_lines(
        silver_claims_df
    )

    overwrite_delta_table(
        df=service_lines_df,
        table_name=silver_table(
            "carrier_service_lines"
        ),
    )

    # -------------------------
    # Gold
    # -------------------------

    gold_df = build_carrier_yearly_summary(
        service_lines_df
    )

    overwrite_delta_table(
        df=gold_df,
        table_name=gold_table(
            "carrier_yearly_summary"
        ),
    )


if __name__ == "__main__":
    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "An active Databricks Spark session is required."
        )

    run_carrier_pipeline(spark)