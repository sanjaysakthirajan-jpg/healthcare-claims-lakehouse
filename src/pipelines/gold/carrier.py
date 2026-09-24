"""Gold analytics transformations for CMS Carrier Claims."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_carrier_yearly_summary(
    service_lines_df: DataFrame,
) -> DataFrame:
    """
    Build yearly Carrier utilization and payment metrics.

    Grain:
        One row per service year.
    """

    return (
        service_lines_df
        .withColumn(
            "service_year",
            F.year(F.col("claim_from_date")),
        )
        .groupBy("service_year")
        .agg(
            F.count("*").alias(
                "service_line_count"
            ),
            F.countDistinct("claim_id").alias(
                "distinct_claim_count"
            ),
            F.countDistinct("beneficiary_id").alias(
                "distinct_beneficiary_count"
            ),
            F.countDistinct("hcpcs_code").alias(
                "distinct_hcpcs_count"
            ),
            F.countDistinct(
                "performing_provider_npi"
            ).alias(
                "distinct_provider_count"
            ),
            F.round(
                F.sum("line_payment_amount"),
                2,
            ).alias(
                "total_line_payment_amount"
            ),
            F.round(
                F.sum("allowed_charge_amount"),
                2,
            ).alias(
                "total_allowed_charge_amount"
            ),
            F.round(
                F.avg("line_payment_amount"),
                2,
            ).alias(
                "avg_line_payment_amount"
            ),
            F.sum(
                F.col(
                    "dq_missing_provider_npi"
                ).cast("int")
            ).alias(
                "missing_provider_npi_count"
            ),
            F.sum(
                F.col(
                    "dq_negative_line_payment"
                ).cast("int")
            ).alias(
                "negative_line_payment_count"
            ),
        )
        .orderBy("service_year")
    )