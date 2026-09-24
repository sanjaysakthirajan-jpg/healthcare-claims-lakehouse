"""Unit tests for PDE Gold transformations."""

from datetime import date

from pyspark.sql import SparkSession

from src.pipelines.gold.pde import build_pde_yearly_summary


def test_build_pde_yearly_summary():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pde-gold-test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    silver_df = spark.createDataFrame(
        [
            (
                "BEN001",
                "PDE001",
                "DRUG001",
                date(2008, 1, 3),
                30.0,
                20,
                10.0,
                120.0,
                False,
            ),
            (
                "BEN001",
                "PDE002",
                "DRUG002",
                date(2008, 2, 5),
                10.0,
                0,
                5.0,
                50.0,
                True,
            ),
            (
                "BEN002",
                "PDE003",
                "DRUG001",
                date(2009, 3, 10),
                20.0,
                30,
                15.0,
                100.0,
                False,
            ),
        ],
        [
            "beneficiary_id",
            "pde_id",
            "product_service_id",
            "service_date",
            "quantity_dispensed",
            "days_supply",
            "patient_payment_amount",
            "total_rx_cost_amount",
            "dq_zero_days_supply",
        ],
    )

    result = build_pde_yearly_summary(
        silver_df
    ).collect()

    assert len(result) == 2

    year_2008 = result[0]
    year_2009 = result[1]

    assert year_2008["service_year"] == 2008
    assert year_2008["prescription_event_count"] == 2
    assert year_2008["distinct_pde_count"] == 2
    assert year_2008["distinct_beneficiary_count"] == 1
    assert year_2008["distinct_product_count"] == 2
    assert year_2008["total_rx_cost"] == 170.0
    assert year_2008["avg_rx_cost"] == 85.0
    assert year_2008["total_patient_payment"] == 15.0
    assert year_2008["avg_patient_payment"] == 7.5
    assert year_2008["total_quantity_dispensed"] == 40.0
    assert year_2008["total_days_supply"] == 20
    assert year_2008["zero_days_supply_records"] == 1

    assert year_2009["service_year"] == 2009
    assert year_2009["prescription_event_count"] == 1
    assert year_2009["distinct_beneficiary_count"] == 1
    assert year_2009["total_rx_cost"] == 100.0
    assert year_2009["zero_days_supply_records"] == 0

    spark.stop()