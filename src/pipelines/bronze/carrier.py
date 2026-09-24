"""Bronze ingestion logic for CMS Carrier Claims."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


IDENTIFIER_COLUMNS = (
    ["DESYNPUF_ID", "CLM_ID", "CLM_FROM_DT", "CLM_THRU_DT"]
    + [f"ICD9_DGNS_CD_{i}" for i in range(1, 9)]
    + [f"PRF_PHYSN_NPI_{i}" for i in range(1, 14)]
    + [f"TAX_NUM_{i}" for i in range(1, 14)]
    + [f"HCPCS_CD_{i}" for i in range(1, 14)]
)

MONETARY_COLUMNS = (
    [f"LINE_NCH_PMT_AMT_{i}" for i in range(1, 14)]
    + [f"LINE_BENE_PTB_DDCTBL_AMT_{i}" for i in range(1, 14)]
    + [f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}" for i in range(1, 14)]
    + [f"LINE_COINSRNC_AMT_{i}" for i in range(1, 14)]
    + [f"LINE_ALOWD_CHRG_AMT_{i}" for i in range(1, 14)]
)

LINE_CODE_COLUMNS = (
    [f"LINE_PRCSG_IND_CD_{i}" for i in range(1, 14)]
    + [f"LINE_ICD9_DGNS_CD_{i}" for i in range(1, 14)]
)


CARRIER_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in IDENTIFIER_COLUMNS]
    + [StructField(column, DoubleType(), True) for column in MONETARY_COLUMNS]
    + [StructField(column, StringType(), True) for column in LINE_CODE_COLUMNS]
)


def read_carrier_source(
    spark: SparkSession,
    source_paths: list[str],
) -> DataFrame:
    """Read Carrier source files using the explicit CMS schema."""

    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(CARRIER_SCHEMA)
        .csv(source_paths)
    )


def add_bronze_metadata(
    df: DataFrame,
) -> DataFrame:
    """Add standard Bronze ingestion metadata."""

    return (
        df
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path"),
        )
    )


def build_carrier_bronze(
    spark: SparkSession,
    source_paths: list[str],
) -> DataFrame:
    """Build the Carrier Bronze DataFrame."""

    source_df = read_carrier_source(
        spark=spark,
        source_paths=source_paths,
    )

    return add_bronze_metadata(source_df)