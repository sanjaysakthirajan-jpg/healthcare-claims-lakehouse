"""Gold analytics transformations for CMS Prescription Drug Events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_pde_yearly_summary(
    silver_df: DataFrame,
) -> DataFrame:
    """Build yearly prescription-drug utilization metrics."""

    return (
        silver_df
        .withColumn(
            "service_year",
            F.year(F.col("service_date")),
        )
        .groupBy("service_year")
        .agg(
            F.count("*").alias(
                "prescription_event_count"
            ),
            F.countDistinct("pde_id").alias(
                "distinct_pde_count"
            ),
            F.countDistinct("beneficiary_id").alias(
                "distinct_beneficiary_count"
            ),
            F.countDistinct("product_service_id").alias(
                "distinct_product_count"
            ),
            F.sum("total_rx_cost_amount").alias(
                "total_rx_cost"
            ),
            F.avg("total_rx_cost_amount").alias(
                "avg_rx_cost"
            ),
            F.sum("patient_payment_amount").alias(
                "total_patient_payment"
            ),
            F.avg("patient_payment_amount").alias(
                "avg_patient_payment"
            ),
            F.sum("quantity_dispensed").alias(
                "total_quantity_dispensed"
            ),
            F.sum("days_supply").alias(
                "total_days_supply"
            ),
            F.sum(
                F.col("dq_zero_days_supply").cast("int")
            ).alias(
                "zero_days_supply_records"
            ),
        )
        .withColumn(
            "avg_rx_cost",
            F.round("avg_rx_cost", 2),
        )
        .withColumn(
            "avg_patient_payment",
            F.round("avg_patient_payment", 2),
        )
        .orderBy("service_year")
    )