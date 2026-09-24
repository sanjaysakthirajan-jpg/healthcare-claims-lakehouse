"""Unit tests for pipeline audit utilities."""

from datetime import datetime, timezone

from src.utils.audit import build_pipeline_audit_record


def test_build_pipeline_audit_record():
    record = build_pipeline_audit_record(
        pipeline_name="inpatient_silver_transformation",
        source_system="CMS_DE_SYNPUF",
        target_table="healthcare_claims.silver.inpatient_claims",
        pipeline_layer="silver",
        status="SUCCESS",
        rows_processed=66773,
    )

    assert record["pipeline_name"] == "inpatient_silver_transformation"
    assert record["source_system"] == "CMS_DE_SYNPUF"
    assert (
        record["target_table"]
        == "healthcare_claims.silver.inpatient_claims"
    )
    assert record["pipeline_layer"] == "silver"
    assert record["status"] == "SUCCESS"
    assert record["rows_processed"] == 66773

    assert isinstance(
        record["run_timestamp"],
        datetime,
    )

    assert (
        record["run_timestamp"].tzinfo
        == timezone.utc
    )