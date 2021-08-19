import unittest
from src.price_zone.s3_trigger_lambda import is_partial_or_full_load


class TestETLTriggerLambda(unittest.TestCase):

    def test_is_new_customer(self):
        file_name = 'ctt_fileName.extension'
        self.assertTrue(is_partial_or_full_load(file_name, 'ctt,itt', 'wtp'))

    def test_is_not_a_new_customer(self):
        file_name = 'fileName.extension'
        result1, result2, result3 = is_partial_or_full_load(file_name, 'ctt,itt', 'wtp')
        self.assertFalse(result1)
        self.assertFalse(result2)
        self.assertEqual(result3, '')

    def test_file_is_a_full_export(self):
        file_name = 'wtp.extension'
        result1, result2, result3 = is_partial_or_full_load(file_name, 'ctt,itt', 'wtp')
        self.assertFalse(result1)
        self.assertTrue(result2)
        self.assertEqual(result3, 'wtp')

    def test_file_is_a_full_export_file_name_is_upper_case(self):
        file_name = 'WTP_20210612_018-841398_9dbe1840-d13d-5eb1-91e4-417e95c1a187.csv.gz'
        result1, result2, result3 = is_partial_or_full_load(file_name, 'ctt,itt', 'wtp')
        self.assertFalse(result1)
        self.assertTrue(result2)
        self.assertEqual(result3, 'wtp')

    def test_file_is_a_partial_export_file_name_is_upper_case(self):
        file_name = 'CTT_20210612_018-841398_9dbe1840-d13d-5eb1-91e4-417e95c1a187.csv.gz'
        result1, result2, result3 = is_partial_or_full_load(file_name, 'ctt,itt', 'wtp')
        self.assertTrue(result1)
        self.assertFalse(result2)
        self.assertEqual(result3, 'ctt')


if __name__ == '__main__':
    unittest.main()
