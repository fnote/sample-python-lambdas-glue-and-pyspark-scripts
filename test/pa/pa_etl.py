import unittest
import pandas as pd
from src.pa.pa_etl_script import validate_price

class TestPAETLScriptMethods(unittest.TestCase):

    def test_when_no_negative_prices(self):

        input_df = pd.DataFrame.from_dict({
            'LOCAL_REFERENCE_PRICE': [13.44 , 70.00],
            'ITEM_ID': ['001', '002']
        })

        self.assertIsNone(validate_price(input_df, 'LOCAL_REFERENCE_PRICE'))

    def test_when_negative_prices_present(self):

        input_df = pd.DataFrame.from_dict({
            'LOCAL_REFERENCE_PRICE': [13.44, -70.00],
            'ITEM_ID': ['001', '002']
        })

        with self.assertRaises(ValueError):
            validate_price(input_df, 'LOCAL_REFERENCE_PRICE')

    def test_when_zero_prices_present(self):

        input_df = pd.DataFrame.from_dict({
            'LOCAL_REFERENCE_PRICE': [16, 0.00],
            'ITEM_ID': ['001', '002']
        })

        with self.assertRaises(ValueError):
            validate_price(input_df, 'LOCAL_REFERENCE_PRICE')


if __name__ == '__main__':
    unittest.main()