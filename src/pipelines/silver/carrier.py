"""Silver transformation logic for CMS Carrier Claims."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.checks import (
    add_invalid_date_order_flag,
    add_missing_value_flag,
    add_negative_value_flag,
)


def build_carrier_claims_silver(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform Carrier Bronze data into the claim-level Silver dataset.

    The wide service-line columns are intentionally preserved here.
    Service-line normalization is handled separately.
    """

    silver_df = (
        bronze_df
        .withColumn(
            "claim_from_date",
            F.to_date(
                F.col("CLM_FROM_DT"),
                "yyyyMMdd",
            ),
        )
        .withColumn(
            "claim_thru_date",
            F.to_date(
                F.col("CLM_THRU_DT"),
                "yyyyMMdd",
            ),
        )
        .withColumnRenamed(
            "DESYNPUF_ID",
            "beneficiary_id",
        )
        .withColumnRenamed(
            "CLM_ID",
            "claim_id",
        )
    )

    silver_df = add_missing_value_flag(
        silver_df,
        column_name="beneficiary_id",
        flag_name="dq_missing_beneficiary_id",
    )

    silver_df = add_missing_value_flag(
        silver_df,
        column_name="claim_id",
        flag_name="dq_missing_claim_id",
    )

    silver_df = add_missing_value_flag(
        silver_df,
        column_name="claim_from_date",
        flag_name="dq_missing_claim_from_date",
    )

    silver_df = add_missing_value_flag(
        silver_df,
        column_name="claim_thru_date",
        flag_name="dq_missing_claim_thru_date",
    )

    silver_df = add_invalid_date_order_flag(
        silver_df,
        start_date_column="claim_from_date",
        end_date_column="claim_thru_date",
        flag_name="dq_invalid_claim_date_order",
    )

    return silver_df.drop(
        "CLM_FROM_DT",
        "CLM_THRU_DT",
    )


def build_carrier_service_lines(
    claims_df: DataFrame,
) -> DataFrame:
    """
    Normalize the 13 repeating Carrier service-line slots.

    Only service-line slots with a populated HCPCS code are emitted.
    Claim-level records remain preserved separately in carrier_claims.
    """

    service_line_structs = []

    for line_number in range(1, 14):
        service_line_structs.append(
            F.struct(
                F.lit(line_number).alias(
                    "service_line_number"
                ),
                F.col(
                    f"HCPCS_CD_{line_number}"
                ).alias("hcpcs_code"),
                F.col(
                    f"PRF_PHYSN_NPI_{line_number}"
                ).alias("performing_provider_npi"),
                F.col(
                    f"TAX_NUM_{line_number}"
                ).alias("tax_number"),
                F.col(
                    f"LINE_PRCSG_IND_CD_{line_number}"
                ).alias("processing_indicator_code"),
                F.col(
                    f"LINE_ICD9_DGNS_CD_{line_number}"
                ).alias("line_diagnosis_code"),
                F.col(
                    f"LINE_NCH_PMT_AMT_{line_number}"
                ).alias("line_payment_amount"),
                F.col(
                    f"LINE_BENE_PTB_DDCTBL_AMT_{line_number}"
                ).alias("beneficiary_deductible_amount"),
                F.col(
                    f"LINE_BENE_PRMRY_PYR_PD_AMT_{line_number}"
                ).alias("primary_payer_paid_amount"),
                F.col(
                    f"LINE_COINSRNC_AMT_{line_number}"
                ).alias("coinsurance_amount"),
                F.col(
                    f"LINE_ALOWD_CHRG_AMT_{line_number}"
                ).alias("allowed_charge_amount"),
            )
        )

    service_lines_df = (
        claims_df
        .select(
            "beneficiary_id",
            "claim_id",
            "claim_from_date",
            "claim_thru_date",
            F.explode(
                F.array(*service_line_structs)
            ).alias("service_line"),
        )
        .select(
            "beneficiary_id",
            "claim_id",
            "claim_from_date",
            "claim_thru_date",
            "service_line.*",
        )
        .filter(
            F.col("hcpcs_code").isNotNull()
            & (F.trim(F.col("hcpcs_code")) != "")
        )
    )

    service_lines_df = add_missing_value_flag(
        service_lines_df,
        column_name="performing_provider_npi",
        flag_name="dq_missing_provider_npi",
    )

    service_lines_df = add_negative_value_flag(
        service_lines_df,
        column_name="line_payment_amount",
        flag_name="dq_negative_line_payment",
    )

    return service_lines_df