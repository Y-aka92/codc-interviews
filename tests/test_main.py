import sys
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
sys.path.append('../src')
from main import filter_clients_by_country


class TestMainMethods(unittest.TestCase):
    # setUp method to initialize SparkSession and create a test DataFrame
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestApp") \
            .getOrCreate()
        # Create test data
        self.test_data = [Row(country="Netherlands"), Row(country="United Kingdom"), Row(country="Germany")]
        self.df = self.spark.createDataFrame(self.test_data)

    # Method to test the filter_clients_by_country function
    def test_filter_clients_by_country(self):
        # Call the function with the test DataFrame
        resultaat = filter_clients_by_country(self.df, ["Netherlands", "United Kingdom"])
        # Expected output
        expected_data = [Row(country="Netherlands"), Row(country="United Kingdom")]
        expected_df = self.spark.createDataFrame(expected_data)
        # Assert if the result is as expected
        self.assertEqual(resultaat.collect(), expected_df.collect())

    # tearDown method to stop the SparkSession
    def tearDown(self):
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
