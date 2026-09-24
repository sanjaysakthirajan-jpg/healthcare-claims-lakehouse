# Databricks notebook source

from pyspark.sql import SparkSession

from src.pipelines.run_pde_pipeline import (
    run_pde_pipeline,
)

spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError(
        "No active Spark session was found."
    )

run_pde_pipeline(spark)

print(
    "PDE pipeline completed successfully."
)