# Databricks notebook source

from pyspark.sql import SparkSession

from src.pipelines.run_carrier_pipeline import (
    run_carrier_pipeline,
)


spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError(
        "No active Spark session was found."
    )


run_carrier_pipeline(spark)

print(
    "Carrier pipeline completed successfully."
)