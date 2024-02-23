from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
import unittest

SparkSession.builder = DatabricksSession.builder
SparkSession.builder.getOrCreate()


class TestExample(unittest.TestCase):
    def test_addition(self):
        self.assertEqual(1 + 1, 2)

    def test_subtraction(self):
        self.assertEqual(3 - 2, 1)


if __name__ == "__main__":
    unittest.main()
