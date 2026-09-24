"""Silver transformation logic for CMS Prescription Drug Events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_pde_silver(
    bronze_df: DataFrame,
) -> DataFrame:
    """Transform Bronze PDE records into the standardized Silver model."""

    return (
        bronze_df
        .withColumnRenamed(
            "DESYNPUF_ID",
            "beneficiary_id",
        )
        .withColumnRenamed(
            "PDE_ID",
            "pde_id",
        )
        .withColumnRenamed(
            "PROD_SRVC_ID",
            "product_service_id",
        )
        .withColumnRenamed(
            "QTY_DSPNSD_NUM",
            "quantity_dispensed",
        )
        .withColumnRenamed(
            "DAYS_SUPLY_NUM",
            "days_supply",
        )
        .withColumnRenamed(
            "PTNT_PAY_AMT",
            "patient_payment_amount",
        )
        .withColumnRenamed(
            "TOT_RX_CST_AMT",
            "total_rx_cost_amount",
        )
        .withColumn(
            "service_date",
            F.to_date(
                F.col("SRVC_DT"),
                "yyyyMMdd",
            ),
        )
        .withColumn(
            "dq_missing_pde_id",
            F.col("pde_id").isNull()
            | (F.trim(F.col("pde_id")) == ""),
        )
        .withColumn(
            "dq_missing_beneficiary_id",
            F.col("beneficiary_id").isNull()
            | (F.trim(F.col("beneficiary_id")) == ""),
        )
        .withColumn(
            "dq_missing_service_date",
            F.col("service_date").isNull(),
        )
        .withColumn(
            "dq_missing_product_service_id",
            F.col("product_service_id").isNull()
            | (F.trim(F.col("product_service_id")) == ""),
        )
        .withColumn(
            "dq_zero_days_supply",
            F.col("days_supply") == 0,
        )
        .withColumn(
            "dq_negative_quantity",
            F.col("quantity_dispensed") < 0,
        )
        .withColumn(
            "dq_negative_patient_payment",
            F.col("patient_payment_amount") < 0,
        )
        .withColumn(
            "dq_negative_total_rx_cost",
            F.col("total_rx_cost_amount") < 0,
        )
        .withColumn(
            "_silver_processed_at",
            F.current_timestamp(),
        )
        .drop("SRVC_DT")
    )