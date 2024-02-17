import unittest
from unittest.mock import Mock
from main import filter_clients_by_country

class TestMainMethods(unittest.TestCase):
    
    def setUp(self):
        # Stel een mock SparkSession in als dat nodig is voor de functies
        self.mock_spark = Mock()
        self.mock_df = Mock()
    
    def test_filter_clients_by_country(self):
        # Stel je verwachte resultaat vast
        self.mock_df.filter.return_value = 'gefilterde DataFrame'
        
        # Voer de functie uit met mock objecten
        resultaat = filter_clients_by_country(self.mock_df, ["Netherlands", "United Kingdom"])
        
        # Controleer of de mock filter-functie werd aangeroepen met de juiste parameters
        self.mock_df.filter.assert_called_with("country IN ('Netherlands', 'United Kingdom')")
        self.assertEqual(resultaat, 'gefilterde DataFrame')

if __name__ == '__main__':
    unittest.main()
