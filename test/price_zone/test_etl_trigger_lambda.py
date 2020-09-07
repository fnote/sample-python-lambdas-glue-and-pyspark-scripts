import unittest
from src.price_zone.s3_trigger_lambda import is_newCustomer, NEW_CUSTOMER_FILE_PREFIX


class TestETLTriggerLambda(unittest.TestCase):

    def test_is_new_customer(self):
        fileName = NEW_CUSTOMER_FILE_PREFIX + 'fileName.extension'
        self.assertTrue(is_newCustomer(fileName))

    def test_is_not_a_new_customer(self):
        fileName = 'fileName.extension'
        self.assertFalse(is_newCustomer(fileName))


if __name__ == '__main__':
    unittest.main()
