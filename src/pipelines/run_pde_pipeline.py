"""Databricks execution entry point for the PDE pipeline."""

from pyspark.sql import SparkSession

from config.project_config import (
    PDE_RAW_PATH,
    bronze_table,
    gold_table,
    silver_table,
)
from src.pipelines.bronze.pde import (
    build_pde_bronze,
)
from src.pipelines.gold.pde import (
    build_pde_yearly_summary,
)
from src.pipelines.silver.pde import (
    build_pde_silver,
)
from src.utils.delta import overwrite_delta_table


def run_pde_pipeline(
    spark: SparkSession,
) -> None:
    """
    Execute the PDE Bronze → Silver → Gold pipeline.
    """
    bronze_df = build_pde_bronze(
        spark=spark,
        source_paths=[PDE_RAW_PATH],
    )

    overwrite_delta_table(
        df=bronze_df,
        table_name=bronze_table(
            "prescription_drug_events"
        ),
    )

    silver_df = build_pde_silver(
        bronze_df
    )

    overwrite_delta_table(
        df=silver_df,
        table_name=silver_table(
            "prescription_drug_events"
        ),
    )

    gold_df = build_pde_yearly_summary(
        silver_df
    )

    overwrite_delta_table(
        df=gold_df,
        table_name=gold_table(
            "prescription_drug_yearly_summary"
        ),
    )


if __name__ == "__main__":
    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError(
            "An active Databricks Spark session is required."
        )

    run_pde_pipeline(spark)