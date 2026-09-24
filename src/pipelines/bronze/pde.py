"""Bronze ingestion logic for CMS Prescription Drug Events."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PDE_SCHEMA = StructType(
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


def read_pde_source(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    """Read CMS DE-SynPUF PDE data using an explicit source schema."""

    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(PDE_SCHEMA)
        .csv(source_path)
    )


def add_bronze_metadata(df: DataFrame) -> DataFrame:
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


def build_pde_bronze(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    """Build the PDE Bronze DataFrame."""

    source_df = read_pde_source(
        spark=spark,
        source_path=source_path,
    )

    return add_bronze_metadata(source_df)