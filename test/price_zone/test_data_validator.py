import unittest
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.price_zone import validator
from src.price_zone.constants import SUPC_LENGTH, CO_CUST_NBR_LENGTH, PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE, \
    DATE_FORMAT_REGEX, INPUT_DATE_FORMAT


class TestSparkDataframeValidator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    def test_null_data_for_supc(self):
        """PRCP-2012"""

        data = [['38104612', None, 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_empty_data_for_supc(self):
        """PRCP-2012"""

        data = [['38104612', '', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_non_numeric_data_for_supc(self):
        """PRCP-2012"""

        data = [['38104612', '1#$%^&', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_data_length_for_supc(self):
        """PRCP-2012"""

        data = [['38104612', '1234567890', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column_length(df, 'supc', SUPC_LENGTH)

    def test_null_data_for_co_cust_nbr(self):
        """PRCP-2011"""

        data = [[None, '1272772', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

    def test_empty_data_for_co_cust_nbr(self):
        """PRCP-2011"""

        data = [['', '1272772', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

    def test_non_numeric_data_for_co_cust_nbr(self):
        """PRCP-2011"""

        data = [['06857936$', '1272772', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

    def test_data_length_for_co_cust_nbr(self):
        """PRCP-2011"""

        data = [['068123456789012345', '1234567890', 5, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column_length(df, 'co_cust_nbr', CO_CUST_NBR_LENGTH)

    def test_null_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['068579369', '1272772', None, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_empty_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['068579369', '1272772', '', '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_non_numeric_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['068579369', '1272772', '&', '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_range_of_price_zone_with_value_less_than_min(self):
        """PRCP-2013"""

        data = [['068579369', '1272772', 0, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_range_of_price_zone_with_value_greater_than_max(self):
        """PRCP-2013"""

        data = [['068579369', '1272772', 6, '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_with_one_invalid_co_cust_nbr_and_valid_co_cust_nbr_list(self):
        """PRCP-2016"""

        data = [['', '4119061', '5', '5/15/2020'],
                ['11810622', '9002908', '1', '5/15/2020'],
                ['19666867', '3555349', '1', '5/15/2020'],
                ['68752266', '4518403', '5', '5/15/2020']]
        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

    def test_data_with_one_invalid_supc_and_valid_supc_list(self):
        """PRCP-2017"""

        data = [['11810622', '9002908', '1', '5/15/2020'],
                ['19666867', '3555349', '1', '5/15/2020'],
                ['11480111', '1#$%^&', '5', '5/15/2020'],
                ['68752266', '4518403', '5', '5/15/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_data_with_one_invalid_price_zone_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['11480111', '4119061', None, '5/15/2020'],
                ['11810622', '9002908', '1', '5/15/2020'],
                ['19666867', '3555349', '1', '5/15/2020'],
                ['68752266', '4518403', '5', '5/15/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_with_one_invalid_price_zone_out_of_range_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['11480111', '4119061', 1, '5/15/2020'],
                ['11810622', '9002908', 11, '5/15/2020'],
                ['19666867', '3555349', 1, '5/15/2020'],
                ['68752266', '4518403', 5, '5/15/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)])

        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_containing_one_empty_row(self):
        """PRCP-2065"""

        data = [['11480111', '4119061', '2', '5/15/2020'],
                ['11810622', '9002908', '1', '5/15/2020'],
                ['19666867', '3555349', '1', '5/15/2020'],
                ['68752266', '4518403', '5', '5/15/2020'],
                ['', '', '', '']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_containing_one_null_row(self):
        """PRCP-2065"""

        data = [['11480111', '4119061', '2', '5/15/2020'],
                ['11810622', '9002908', '1', '5/15/2020'],
                ['19666867', '3555349', '1', '5/15/2020'],
                ['68752266', '4518403', '5', '5/15/2020'],
                [None, None, None,  None]]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'co_cust_nbr')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_null_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['68752267', '4518403', 5, None]]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_empty_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['68752267', '4518403', 5, '']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_non_numeric_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['68752267', '4518403', 5, 'abc']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_1_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/mm/yyyy"""

        data = [['68752267', '4518403', 5, '25/10/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_2_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/yyyy/mm"""

        data = [['68752267', '4518403', 5, '25/2020/10']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_3_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format mm/yyyy/dd"""

        data = [['68752267', '4518403', 5, '12/2020/26']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_4_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/mm/dd"""

        data = [['68752267', '4518403', 5, '2020/08/28']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_5_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/dd/mm"""

        data = [['68752267', '4518403', 5, '2020/22/07']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_6_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format Month/dd/yyyy"""

        data = [['68752267', '4518403', 5, 'June/10/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_7_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format 01 and 1"""

        data = [['68752267', '4518403', 5, '01/15/2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_8_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format year as yy"""

        data = [['68752267', '4518403', 5, '01/15/20']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_9_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format No split"""

        data = [['68752267', '4518403', 5, '1 15 2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_10_non_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format wrong split"""

        data = [['68752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_data_with_one_invalid_effective_date_and_valid_effective_date_list(self):
        """PRCP-2020"""

        data = [['11810622', '9002908', 1, '5/15/2020'],
                ['19666867', '3555349', 1, '5/15/2020'],
                ['11480111', '4518408', 5, '5/15/2020'],
                ['68752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX)

    def test_data_with_one_invalid_effective_date_value_and_valid_effective_date_list(self):

        data = [['11810622', '9002908', 1, '2/15/2019'],
                ['19666867', '3555349', 1, '2/15/2019'],
                ['11480111', '4518408', 5, '2/15/2019'],
                ['68752267', '4518403', 5, '2/30/2019']]

        schema = StructType([
            StructField("co_cust_nbr", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date_str", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_and_get_as_date(df, 'effective_date_str', 'effective_date', INPUT_DATE_FORMAT)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

if __name__ == '__main__':
    unittest.main(verbosity=2, warnings='ignore')