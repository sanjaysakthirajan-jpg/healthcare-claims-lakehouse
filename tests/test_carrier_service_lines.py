"""Unit tests for Carrier service-line normalization."""

from pyspark.sql import SparkSession

from src.pipelines.silver.carrier import (
    build_carrier_service_lines,
)


def test_carrier_service_line_normalization():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("carrier-service-line-test")
        .getOrCreate()
    )

    row = {
        "beneficiary_id": "BEN001",
        "claim_id": "CLM001",
        "claim_from_date": None,
        "claim_thru_date": None,
    }

    # Create all 13 service-line slots.
    for i in range(1, 14):
        row[f"HCPCS_CD_{i}"] = None
        row[f"PRF_PHYSN_NPI_{i}"] = None
        row[f"TAX_NUM_{i}"] = None
        row[f"LINE_PRCSG_IND_CD_{i}"] = None
        row[f"LINE_ICD9_DGNS_CD_{i}"] = None
        row[f"LINE_NCH_PMT_AMT_{i}"] = None
        row[f"LINE_BENE_PTB_DDCTBL_AMT_{i}"] = None
        row[f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}"] = None
        row[f"LINE_COINSRNC_AMT_{i}"] = None
        row[f"LINE_ALOWD_CHRG_AMT_{i}"] = None

    # Service line 1: valid record.
    row["HCPCS_CD_1"] = "99213"
    row["PRF_PHYSN_NPI_1"] = "1111111111"
    row["LINE_NCH_PMT_AMT_1"] = 75.0

    # Service line 3: intentionally contains DQ issues.
    row["HCPCS_CD_3"] = "80053"
    row["PRF_PHYSN_NPI_3"] = None
    row["LINE_NCH_PMT_AMT_3"] = -40.0

    schema = (
        "beneficiary_id string, "
        "claim_id string, "
        "claim_from_date date, "
        "claim_thru_date date, "
        + ", ".join(
            [
                field
                for i in range(1, 14)
                for field in [
                    f"HCPCS_CD_{i} string",
                    f"PRF_PHYSN_NPI_{i} string",
                    f"TAX_NUM_{i} string",
                    f"LINE_PRCSG_IND_CD_{i} string",
                    f"LINE_ICD9_DGNS_CD_{i} string",
                    f"LINE_NCH_PMT_AMT_{i} double",
                    f"LINE_BENE_PTB_DDCTBL_AMT_{i} double",
                    f"LINE_BENE_PRMRY_PYR_PD_AMT_{i} double",
                    f"LINE_COINSRNC_AMT_{i} double",
                    f"LINE_ALOWD_CHRG_AMT_{i} double",
                ]
            ]
        )
    )

    claims_df = spark.createDataFrame(
        [row],
        schema=schema,
    )

    result = (
        build_carrier_service_lines(claims_df)
        .orderBy("service_line_number")
        .collect()
    )

    # Only the two populated HCPCS slots should become rows.
    assert len(result) == 2

    # Valid service line.
    assert result[0]["service_line_number"] == 1
    assert result[0]["hcpcs_code"] == "99213"
    assert (
        result[0]["performing_provider_npi"]
        == "1111111111"
    )
    assert result[0]["line_payment_amount"] == 75.0

    assert result[0]["dq_missing_provider_npi"] is False
    assert result[0]["dq_negative_line_payment"] is False

    # Service line containing DQ issues.
    assert result[1]["service_line_number"] == 3
    assert result[1]["hcpcs_code"] == "80053"
    assert result[1]["performing_provider_npi"] is None
    assert result[1]["line_payment_amount"] == -40.0

    assert result[1]["dq_missing_provider_npi"] is True
    assert result[1]["dq_negative_line_payment"] is True

    spark.stop()