"""Unit tests for the Carrier pipeline runner."""

from unittest.mock import MagicMock, call, patch

from src.pipelines.run_carrier_pipeline import (
    run_carrier_pipeline,
)


@patch(
    "src.pipelines.run_carrier_pipeline.overwrite_delta_table"
)
@patch(
    "src.pipelines.run_carrier_pipeline.build_carrier_yearly_summary"
)
@patch(
    "src.pipelines.run_carrier_pipeline.build_carrier_service_lines"
)
@patch(
    "src.pipelines.run_carrier_pipeline.build_carrier_claims_silver"
)
@patch(
    "src.pipelines.run_carrier_pipeline.build_carrier_bronze"
)
def test_run_carrier_pipeline(
    mock_bronze,
    mock_silver,
    mock_service_lines,
    mock_gold,
    mock_write,
):
    spark = MagicMock()

    bronze_df = MagicMock(name="bronze_df")
    silver_df = MagicMock(name="silver_df")
    service_lines_df = MagicMock(name="service_lines_df")
    gold_df = MagicMock(name="gold_df")

    mock_bronze.return_value = bronze_df
    mock_silver.return_value = silver_df
    mock_service_lines.return_value = service_lines_df
    mock_gold.return_value = gold_df

    run_carrier_pipeline(spark)

    mock_bronze.assert_called_once()

    mock_silver.assert_called_once_with(
        bronze_df
    )

    mock_service_lines.assert_called_once_with(
        silver_df
    )

    mock_gold.assert_called_once_with(
        service_lines_df
    )

    assert mock_write.call_count == 4

    written_dataframes = [
        write_call.kwargs["df"]
        for write_call in mock_write.call_args_list
    ]

    assert written_dataframes == [
        bronze_df,
        silver_df,
        service_lines_df,
        gold_df,
    ]