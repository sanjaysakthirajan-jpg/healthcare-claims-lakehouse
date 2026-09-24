"""Unit tests for reusable Spark data-quality checks."""

import pytest
from pyspark.sql import SparkSession

from src.quality.checks import (
    add_invalid_date_order_flag,
    add_missing_value_flag,
    add_negative_value_flag,
    duplicate_key_count,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("healthcare-lakehouse-tests")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()


def test_duplicate_key_count(spark):
    df = spark.createDataFrame(
        [
            ("CLAIM001", 1),
            ("CLAIM001", 1),
            ("CLAIM001", 2),
            ("CLAIM002", 1),
        ],
        ["claim_id", "segment"],
    )

    result = duplicate_key_count(
        df,
        ["claim_id", "segment"],
    )

    assert result == 1


def test_missing_value_flag(spark):
    df = spark.createDataFrame(
        [
            ("BEN001",),
            (None,),
            ("",),
        ],
        ["beneficiary_id"],
    )

    result = add_missing_value_flag(
        df,
        "beneficiary_id",
        "dq_missing_beneficiary_id",
    )

    flags = [
        row["dq_missing_beneficiary_id"]
        for row in result.collect()
    ]

    assert flags == [False, True, True]


def test_negative_value_flag(spark):
    df = spark.createDataFrame(
        [
            (100.0,),
            (0.0,),
            (-25.0,),
        ],
        ["claim_payment_amount"],
    )

    result = add_negative_value_flag(
        df,
        "claim_payment_amount",
        "is_negative_payment",
    )

    flags = [
        row["is_negative_payment"]
        for row in result.collect()
    ]

    assert flags == [False, False, True]


def test_invalid_date_order_flag(spark):
    df = spark.createDataFrame(
        [
            ("2026-01-01", "2026-01-05"),
            ("2026-02-10", "2026-02-01"),
        ],
        ["start_date", "end_date"],
    )

    result = (
        df
        .selectExpr(
            "to_date(start_date) AS start_date",
            "to_date(end_date) AS end_date",
        )
    )

    result = add_invalid_date_order_flag(
        result,
        "start_date",
        "end_date",
        "dq_invalid_date_order",
    )

    flags = [
        row["dq_invalid_date_order"]
        for row in result.collect()
    ]

    assert flags == [False, True]