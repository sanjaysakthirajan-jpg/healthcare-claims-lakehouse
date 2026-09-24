"""Unit tests for the Carrier Silver transformation."""

from pyspark.sql import SparkSession

from src.pipelines.silver.carrier import (
    build_carrier_claims_silver,
)


def test_carrier_claims_silver_transformation():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("carrier-silver-test")
        .getOrCreate()
    )

    source_data = [
        (
            "BEN001",
            "CLM001",
            "20080101",
            "20080105",
        ),
        (
            "BEN002",
            "CLM002",
            "20080210",
            "20080201",
        ),
    ]

    bronze_df = spark.createDataFrame(
        source_data,
        [
            "DESYNPUF_ID",
            "CLM_ID",
            "CLM_FROM_DT",
            "CLM_THRU_DT",
        ],
    )

    result_df = build_carrier_claims_silver(
        bronze_df
    )

    rows = {
        row["claim_id"]: row
        for row in result_df.collect()
    }

    assert "beneficiary_id" in result_df.columns
    assert "claim_id" in result_df.columns
    assert "claim_from_date" in result_df.columns
    assert "claim_thru_date" in result_df.columns

    assert "CLM_FROM_DT" not in result_df.columns
    assert "CLM_THRU_DT" not in result_df.columns

    assert (
        rows["CLM001"]["beneficiary_id"]
        == "BEN001"
    )

    assert (
        rows["CLM001"]["dq_invalid_claim_date_order"]
        is False
    )

    assert (
        rows["CLM002"]["dq_invalid_claim_date_order"]
        is True
    )

    spark.stop()
    