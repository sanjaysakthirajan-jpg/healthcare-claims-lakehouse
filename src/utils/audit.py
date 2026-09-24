"""Reusable pipeline audit utilities."""

from datetime import datetime, timezone


def build_pipeline_audit_record(
    pipeline_name: str,
    source_system: str,
    target_table: str,
    pipeline_layer: str,
    status: str,
    rows_processed: int,
) -> dict:
    """
    Build a standardized pipeline audit record.

    Parameters
    ----------
    pipeline_name:
        Name of the pipeline being executed.

    source_system:
        Upstream source system.

    target_table:
        Fully qualified target table name.

    pipeline_layer:
        Lakehouse layer such as bronze, silver, or gold.

    status:
        Pipeline execution status.

    rows_processed:
        Number of rows written or produced by the pipeline.
    """

    return {
        "pipeline_name": pipeline_name,
        "source_system": source_system,
        "target_table": target_table,
        "pipeline_layer": pipeline_layer,
        "status": status,
        "rows_processed": rows_processed,
        "run_timestamp": datetime.now(timezone.utc),
    }