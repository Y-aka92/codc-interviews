import unittest
import pandas as pd  # Importeer Pandas hier
from main import filter_clients_by_country

class TestMainMethods(unittest.TestCase):

    def test_filter_clients_by_country_basic(self):
        # Stel een eenvoudige DataFrame voor voor testdoeleinden
        test_df = pd.DataFrame({
            'country': ['Netherlands', 'United Kingdom', 'France', 'Germany']
        })

        # Voer de functie uit
        resultaat = filter_clients_by_country(test_df, ["Netherlands", "United Kingdom"])

        # Controleer of de functie correct filtert
        expected_df = pd.DataFrame({
            'country': ['Netherlands', 'United Kingdom']
        })

        pd.testing.assert_frame_equal(resultaat, expected_df)

if __name__ == '__main__':
    unittest.main()
