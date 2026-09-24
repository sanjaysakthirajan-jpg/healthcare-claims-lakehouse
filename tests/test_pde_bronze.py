"""Unit tests for the PDE Bronze pipeline."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
)

from src.pipelines.bronze.pde import PDE_SCHEMA


def test_pde_schema_field_count():
    assert len(PDE_SCHEMA.fields) == 8


def test_pde_identifier_fields_are_strings():
    assert isinstance(
        PDE_SCHEMA["DESYNPUF_ID"].dataType,
        StringType,
    )

    assert isinstance(
        PDE_SCHEMA["PDE_ID"].dataType,
        StringType,
    )

    assert isinstance(
        PDE_SCHEMA["PROD_SRVC_ID"].dataType,
        StringType,
    )


def test_pde_numeric_fields_have_expected_types():
    assert isinstance(
        PDE_SCHEMA["QTY_DSPNSD_NUM"].dataType,
        DoubleType,
    )

    assert isinstance(
        PDE_SCHEMA["DAYS_SUPLY_NUM"].dataType,
        IntegerType,
    )

    assert isinstance(
        PDE_SCHEMA["PTNT_PAY_AMT"].dataType,
        DoubleType,
    )

    assert isinstance(
        PDE_SCHEMA["TOT_RX_CST_AMT"].dataType,
        DoubleType,
    )