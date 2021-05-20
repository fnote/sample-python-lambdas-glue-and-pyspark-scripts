import unittest
from src.price_zone.s3_trigger_lambda import is_partial_or_full_load


class TestETLTriggerLambda(unittest.TestCase):

    def test_is_new_customer(self):
        fileName = 'ctt_fileName.extension'
        self.assertTrue(is_partial_or_full_load(fileName, 'ctt,itt'))

    def test_is_not_a_new_customer(self):
        fileName = 'fileName.extension'
        self.assertFalse(is_partial_or_full_load(fileName, 'ctt,itt'))


if __name__ == '__main__':
    unittest.main()
