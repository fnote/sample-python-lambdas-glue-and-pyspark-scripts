import unittest

import pyspark
from pyspark.sql.types import StructType, StructField, StringType

from src.price_zone.constants import DATE_FORMAT_REGEX, INPUT_DATE_FORMAT
from src.price_zone.validator import validate_date_format

valid_date_format_1 = 'yyyy-MM-dd HH:mm:ss.S*'
valid_date_format_2 = 'yyyy-MM-dd HH:mm:ss'


class TestDateRegex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    def test_date_format_regex_for_valid_format_1(self):
        """Valid input date format"""
         
        data = [['2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        try:
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        except ValueError:
            self.fail('should fail. Received Value error for valid date format')

    def test_date_format_regex_for_valid_format_2(self):
        """Valid input date format """
        
        data = [['2020-08-06 00:00:00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        try:
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        except ValueError:
            self.fail('should fail. Received Value error for valid date format')

    def test_date_format_regex_for_invalid_format_1(self):
        """Invalid date format with decimal point only"""

        data = [['2020-08-06 00:00:00.']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_2(self):
        """Invalid date format with non-numeric values after decimal point"""
        
        data = [['2020-08-06 00:00:00.ddd']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
            
    def test_date_format_regex_for_invalid_format_3(self):
        """Invalid date format with missing time value"""
        
        data = [['2020-08-06 00:00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_4(self):
        """Invalid date format with missing all time values"""
        
        data = [['2020-08-06']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_5(self):
        """Invalid date format with invalid date separator"""
        
        data = [['2020/08/06 00:00:00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_6(self):
        """Invalid date format with no separator between date and time"""
        
        data = [['2020-08-0600:00:00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_7(self):
        """Invalid date format with no date separator"""
        
        data = [['20200806 00:00:00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_date_format_regex_for_invalid_format_8(self):
        """Invalid separator for time"""

        data = [['2020-08-06 00 00 00']]
        schema = StructType([
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
            
    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


if __name__ == '__main__':
    unittest.main()
