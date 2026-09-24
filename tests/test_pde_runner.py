from unittest.mock import MagicMock, patch

from src.pipelines.run_pde_pipeline import (
    run_pde_pipeline,
)


@patch(
    "src.pipelines.run_pde_pipeline."
    "overwrite_delta_table"
)
@patch(
    "src.pipelines.run_pde_pipeline."
    "build_pde_yearly_summary"
)
@patch(
    "src.pipelines.run_pde_pipeline."
    "build_pde_silver"
)
@patch(
    "src.pipelines.run_pde_pipeline."
    "build_pde_bronze"
)
def test_run_pde_pipeline(
    mock_bronze,
    mock_silver,
    mock_gold,
    mock_write,
):
    spark = MagicMock()

    bronze_df = MagicMock()
    silver_df = MagicMock()
    gold_df = MagicMock()

    mock_bronze.return_value = bronze_df
    mock_silver.return_value = silver_df
    mock_gold.return_value = gold_df

    run_pde_pipeline(spark)

    mock_bronze.assert_called_once()

    mock_silver.assert_called_once_with(
        bronze_df
    )

    mock_gold.assert_called_once_with(
        silver_df
    )

    assert mock_write.call_count == 3