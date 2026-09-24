"""Unit tests for the Carrier Bronze pipeline."""

from pyspark.sql.types import (
    DoubleType,
    StringType,
)

from src.pipelines.bronze.carrier import CARRIER_SCHEMA


def test_carrier_schema_field_count():
    assert len(CARRIER_SCHEMA.fields) == 142


def test_carrier_identifiers_are_strings():
    string_fields = [
        "DESYNPUF_ID",
        "CLM_ID",
        "CLM_FROM_DT",
        "CLM_THRU_DT",
        "ICD9_DGNS_CD_1",
        "PRF_PHYSN_NPI_1",
        "TAX_NUM_1",
        "HCPCS_CD_1",
        "LINE_PRCSG_IND_CD_1",
        "LINE_ICD9_DGNS_CD_1",
    ]

    for field_name in string_fields:
        assert isinstance(
            CARRIER_SCHEMA[field_name].dataType,
            StringType,
        )


def test_carrier_monetary_fields_are_double():
    monetary_fields = [
        "LINE_NCH_PMT_AMT_1",
        "LINE_BENE_PTB_DDCTBL_AMT_1",
        "LINE_BENE_PRMRY_PYR_PD_AMT_1",
        "LINE_COINSRNC_AMT_1",
        "LINE_ALOWD_CHRG_AMT_1",
    ]

    for field_name in monetary_fields:
        assert isinstance(
            CARRIER_SCHEMA[field_name].dataType,
            DoubleType,
        )


def test_all_thirteen_service_line_slots_exist():
    for line_number in range(1, 14):
        assert (
            f"PRF_PHYSN_NPI_{line_number}"
            in CARRIER_SCHEMA.names
        )

        assert (
            f"HCPCS_CD_{line_number}"
            in CARRIER_SCHEMA.names
        )

        assert (
            f"LINE_NCH_PMT_AMT_{line_number}"
            in CARRIER_SCHEMA.names
        )

        assert (
            f"LINE_ICD9_DGNS_CD_{line_number}"
            in CARRIER_SCHEMA.names
        )