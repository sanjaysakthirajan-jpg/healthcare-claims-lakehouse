"""Unit tests for Delta Lake write utilities."""

from unittest.mock import MagicMock

from src.utils.delta import overwrite_delta_table


def test_overwrite_delta_table():
    df = MagicMock()

    format_writer = MagicMock()
    mode_writer = MagicMock()
    option_writer = MagicMock()

    df.write.format.return_value = format_writer
    format_writer.mode.return_value = mode_writer
    mode_writer.option.return_value = option_writer

    overwrite_delta_table(
        df=df,
        table_name="healthcare_claims.silver.test_table",
    )

    df.write.format.assert_called_once_with("delta")

    format_writer.mode.assert_called_once_with(
        "overwrite"
    )

    mode_writer.option.assert_called_once_with(
        "overwriteSchema",
        "true",
    )

    option_writer.saveAsTable.assert_called_once_with(
        "healthcare_claims.silver.test_table"
    )