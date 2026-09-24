"""Central configuration for the Healthcare Claims Lakehouse project."""

# Databricks / Unity Catalog
CATALOG = "healthcare_claims"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUARANTINE_SCHEMA = "quarantine"
OPS_SCHEMA = "ops"


# AWS S3
S3_BUCKET = "healthcare-claims-lakehouse-sanjay"
S3_BASE_PATH = f"s3://{S3_BUCKET}"


# Raw source locations
RAW_CMS_BASE_PATH = f"{S3_BASE_PATH}/raw/cms"

BENEFICIARY_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/beneficiary"
)

INPATIENT_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/inpatient"
)

OUTPATIENT_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/outpatient"
)

CARRIER_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/carrier"
)

PDE_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/prescription_drug_events"
)

PROVIDER_RAW_PATH = (
    f"{RAW_CMS_BASE_PATH}/provider"
)


# Fully qualified table helpers
def bronze_table(table_name: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


def silver_table(table_name: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"


def gold_table(table_name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"


def ops_table(table_name: str) -> str:
    return f"{CATALOG}.{OPS_SCHEMA}.{table_name}"