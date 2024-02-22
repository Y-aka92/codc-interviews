import unittest
import sys
sys.path.append('../src')
from unittest.mock import Mock
from main import filter_clients_by_country

class TestMainMethods(unittest.TestCase):
    
    def setUp(self):
        self.mock_spark = Mock()
        self.mock_df = Mock()
    
    def test_filter_clients_by_country(self):
        self.mock_df.filter.return_value = 'gefilterde DataFrame'
        resultaat = filter_clients_by_country(self.mock_df, ["Netherlands", "United Kingdom"])
        self.mock_df.filter.assert_called_with("country IN ('Netherlands', 'United Kingdom')")
        self.assertEqual(resultaat, 'gefilterde DataFrame')

if __name__ == '__main__':
    unittest.main()
