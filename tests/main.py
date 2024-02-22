from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

SparkSession.builder = DatabricksSession.builder
SparkSession.builder.getOrCreate()

def test_main():
    # Still pending usage of unit tests in datasets
    addition = 1 + 1
    assert addition == 2
