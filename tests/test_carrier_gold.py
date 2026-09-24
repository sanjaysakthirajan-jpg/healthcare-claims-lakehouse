"""Unit tests for Carrier Gold analytics."""

from datetime import date

from pyspark.sql import SparkSession

from src.pipelines.gold.carrier import (
    build_carrier_yearly_summary,
)


def test_carrier_yearly_summary():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("carrier-gold-test")
        .getOrCreate()
    )

    data = [
        (
            "BEN001",
            "CLM001",
            date(2008, 1, 10),
            "99213",
            "1111111111",
            75.0,
            100.0,
            False,
            False,
        ),
        (
            "BEN001",
            "CLM001",
            date(2008, 1, 10),
            "80053",
            "2222222222",
            40.0,
            60.0,
            False,
            False,
        ),
        (
            "BEN002",
            "CLM002",
            date(2008, 5, 20),
            "99213",
            None,
            -10.0,
            20.0,
            True,
            True,
        ),
        (
            "BEN003",
            "CLM003",
            date(2009, 3, 15),
            "93000",
            "3333333333",
            50.0,
            80.0,
            False,
            False,
        ),
    ]

    service_lines_df = spark.createDataFrame(
        data,
        [
            "beneficiary_id",
            "claim_id",
            "claim_from_date",
            "hcpcs_code",
            "performing_provider_npi",
            "line_payment_amount",
            "allowed_charge_amount",
            "dq_missing_provider_npi",
            "dq_negative_line_payment",
        ],
    )

    result = {
        row["service_year"]: row
        for row in (
            build_carrier_yearly_summary(
                service_lines_df
            ).collect()
        )
    }

    assert set(result.keys()) == {2008, 2009}

    year_2008 = result[2008]

    assert year_2008["service_line_count"] == 3
    assert year_2008["distinct_claim_count"] == 2
    assert year_2008["distinct_beneficiary_count"] == 2
    assert year_2008["distinct_hcpcs_count"] == 2
    assert year_2008["distinct_provider_count"] == 2

    assert year_2008["total_line_payment_amount"] == 105.0
    assert year_2008["total_allowed_charge_amount"] == 180.0
    assert year_2008["avg_line_payment_amount"] == 35.0

    assert year_2008["missing_provider_npi_count"] == 1
    assert year_2008["negative_line_payment_count"] == 1

    year_2009 = result[2009]

    assert year_2009["service_line_count"] == 1
    assert year_2009["distinct_claim_count"] == 1
    assert year_2009["distinct_beneficiary_count"] == 1
    assert year_2009["total_line_payment_amount"] == 50.0

    spark.stop()