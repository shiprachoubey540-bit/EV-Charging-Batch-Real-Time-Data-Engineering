"""
processing/batch_transform.py — Spark batch transformations.

Owner: Aleena Antony

Reads cleaned session data and applies analytical transformations
using PySpark.
"""

import os
from dotenv import load_dotenv

load_dotenv()

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ev-charging-batch")


def create_spark_session():
    """
    Create and return a SparkSession for batch processing.
    """
    # TODO: Implement SparkSession creation
    raise NotImplementedError("Implement SparkSession")


def run_batch_transforms(spark, input_path: str, output_path: str) -> None:
    """
    Execute batch transformations on session data.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
    input_path : str
        Path to cleaned session data (CSV or Parquet).
    output_path : str
        Path to write transformed results.
    """
    # TODO: Implement batch transformations
    raise NotImplementedError("Implement batch transforms")


if __name__ == "__main__":
    spark = create_spark_session()
    run_batch_transforms(spark, "data/sample/", "data/output/")
