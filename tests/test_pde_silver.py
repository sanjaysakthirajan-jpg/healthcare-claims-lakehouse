"""Unit tests for the PDE Silver transformation."""

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.pipelines.silver.pde import build_pde_silver


def test_build_pde_silver():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pde-silver-test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("DESYNPUF_ID", StringType(), True),
            StructField("PDE_ID", StringType(), True),
            StructField("SRVC_DT", StringType(), True),
            StructField("PROD_SRVC_ID", StringType(), True),
            StructField("QTY_DSPNSD_NUM", DoubleType(), True),
            StructField("DAYS_SUPLY_NUM", IntegerType(), True),
            StructField("PTNT_PAY_AMT", DoubleType(), True),
            StructField("TOT_RX_CST_AMT", DoubleType(), True),
        ]
    )

    bronze_df = spark.createDataFrame(
        [
            (
                "BEN001",
                "PDE001",
                "20080103",
                "00247037252",
                30.0,
                20,
                10.0,
                120.0,
            ),
            (
                "BEN002",
                "PDE002",
                "20080201",
                "00012345678",
                10.0,
                0,
                5.0,
                50.0,
            ),
        ],
        schema=schema,
    )

    result = build_pde_silver(bronze_df)

    rows = result.orderBy("pde_id").collect()

    first = rows[0]
    second = rows[1]

    assert first["beneficiary_id"] == "BEN001"
    assert first["pde_id"] == "PDE001"
    assert first["product_service_id"] == "00247037252"
    assert first["service_date"] == date(2008, 1, 3)

    assert first["dq_missing_pde_id"] is False
    assert first["dq_missing_beneficiary_id"] is False
    assert first["dq_missing_service_date"] is False
    assert first["dq_missing_product_service_id"] is False
    assert first["dq_zero_days_supply"] is False
    assert first["dq_negative_quantity"] is False
    assert first["dq_negative_patient_payment"] is False
    assert first["dq_negative_total_rx_cost"] is False

    assert second["product_service_id"] == "00012345678"
    assert second["dq_zero_days_supply"] is True

    assert "SRVC_DT" not in result.columns

    spark.stop()