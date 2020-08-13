import unittest
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from src.price_zone import validator
from src.price_zone.constants import SUPC_LENGTH, CUST_NBR_LENGTH, PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE, \
    DATE_FORMAT_REGEX, INPUT_DATE_FORMAT, OUTPUT_DATE_FORMAT, CO_NBR_LENGTH


class TestSparkDataframeValidator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    def test_with_valid_data(self):
        data = [['019', '104612', '1234567', '5', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)
        try:
            validator.validate_column(df, 'customer_id')
            validator.validate_column(df, 'supc')
            validator.validate_column(df, 'price_zone')
            validator.validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

            validator.validate_column_length_less_than(df, 'customer_id', CUST_NBR_LENGTH)
            validator.validate_column_length_less_than(df, 'supc', SUPC_LENGTH)

            df = df.withColumn("price_zone", df["price_zone"].cast(IntegerType()))
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

            validator.validate_and_get_as_date(df, 'eff_from_dttm', 'effective_date', OUTPUT_DATE_FORMAT)

        except ValueError:
            self.fail("Should fail. Received ValueError for valid data")

    def test_null_data_for_supc(self):
        """PRCP-2012"""

        data = [['019', '104612', None, 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_empty_data_for_supc(self):
        """PRCP-2012"""

        data = [['019', '104612', '', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_non_numeric_data_for_supc(self):
        """PRCP-2012"""

        data = [['019', '104612', '1#$%^&', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_data_length_for_supc(self):
        """PRCP-2012"""

        data = [['019', '104612', '1234567890', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column_length_less_than(df, 'supc', SUPC_LENGTH)

    def test_null_data_for_customer_id(self):
        """PRCP-2011"""

        data = [['019', None, '1272772', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

    def test_empty_data_for_customer_id(self):
        """PRCP-2011"""

        data = [['019', '', '1272772', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

    def test_non_numeric_data_for_customer_id(self):
        """PRCP-2011"""

        data = [['019', '06857936$', '1272772', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

    def test_data_length_for_customer_id(self):
        """PRCP-2011"""

        data = [['019', '123456789012345', '1234567890', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column_length_less_than(df, 'customer_id', CUST_NBR_LENGTH)

    def test_null_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['019', '068579369', '1272772', None, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_empty_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['019', '068579369', '1272772', '', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_non_numeric_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['019', '068579369', '1272772', '&', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_range_of_price_zone_with_value_less_than_min(self):
        """PRCP-2013"""

        data = [['019', '068579369', '1272772', 0, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_range_of_price_zone_with_value_greater_than_max(self):
        """PRCP-2013"""

        data = [['019', '068579369', '1272772', 6, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_with_one_invalid_customer_id_and_valid_customer_id_list(self):
        """PRCP-2016"""

        data = [['019', '', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['019', '11810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', '5', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

    def test_data_with_one_invalid_supc_and_valid_supc_list(self):
        """PRCP-2017"""

        data = [['019', '11810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '11480111', '1#$%^&', '5', '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

    def test_data_with_one_invalid_price_zone_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['019', '11480111', '4119061', None, '2020-08-06 00:00:00.000000'],
                ['019', '11810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_with_one_invalid_price_zone_out_of_range_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['019', '11480111', '4119061', 1, '2020-08-06 00:00:00.000000'],
                ['019', '11810622', '9002908', 11, '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', 5, '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)])

        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)

    def test_data_containing_one_empty_row(self):
        """PRCP-2065"""

        data = [['019', '11480111', '4119061', '2', '2020-08-06 00:00:00.000000'],
                ['019', '11810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', '5', '2020-08-06 00:00:00.000000'],
                ['', '', '', '', '']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_data_containing_one_null_row(self):
        """PRCP-2065"""

        data = [['019', '11480111', '4119061', '2', '2020-08-06 00:00:00.000000'],
                ['019', '11810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '68752266', '4518403', '5', '2020-08-06 00:00:00.000000'],
                [None, None, None, None, None]]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'customer_id')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'supc')

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'price_zone')

    def test_null_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '68752267', '4518403', 5, None]]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_empty_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '68752267', '4518403', 5, '']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_non_numeric_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '68752267', '4518403', 5, 'abc']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_1_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/MM/yyyy"""

        data = [['019', '68752267', '4518403', 5, '25/10/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_2_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/yyyy/MM"""

        data = [['019', '68752267', '4518403', 5, '25/2020/10']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_3_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format MM/yyyy/dd"""

        data = [['019', '68752267', '4518403', 5, '12/2020/26']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_4_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/MM/dd"""

        data = [['019', '68752267', '4518403', 5, '2020/08/28']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_5_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/dd/MM"""

        data = [['019', '68752267', '4518403', 5, '2020/22/07']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_6_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format Month/dd/yyyy"""

        data = [['019', '68752267', '4518403', 5, 'June/10/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_7_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format 01 and 1"""

        data = [['019', '68752267', '4518403', 5, '01/15/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_8_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format year as yy"""

        data = [['019', '68752267', '4518403', 5, '01/15/20']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_9_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format No split"""

        data = [['019', '68752267', '4518403', 5, '1 15 2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_10_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format wrong split"""

        data = [['019', '68752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_data_with_one_invalid_effective_date_and_valid_effective_date_list(self):
        """PRCP-2020"""

        data = [['019', '11810622', '9002908', 1, '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['019', '11480111', '4518408', 5, '2020-08-06 00:00:00.000000'],
                ['019', '68752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_data_with_one_invalid_effective_date_value_and_valid_effective_date_list(self):

        data = [['019', '11810622', '9002908', 1, '2020-08-06 00:00:00.000000'],
                ['019', '19666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['019', '11480111', '4518408', 5, '2020-08-06 00:00:00.000000'],
                ['019', '68752267', '4518403', 5, '2/30/2019']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_and_get_as_date(df, 'eff_from_dttm', 'effective_date', INPUT_DATE_FORMAT)

    def test_null_data_for_opco_id(self):

        data = [[None, '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'opco_id')

    def test_empty_data_for_opco_id(self):

        data = [['', '104612', '', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'opco_id')

    def test_non_numeric_data_for_opco_id(self):

        data = [['abc', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'opco_id')

    def test_data_length_for_opco_id(self):

        data = [['0190', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column_length_equals(df, 'opco_id', CO_NBR_LENGTH)

    def test_data_with_one_invalid_opco_id_and_valid_opco_id_list(self):

        data = [['', '123456', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['019', '118106', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '196668', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['0109', '687522', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_column(df, 'opco_id')

    def test_1_date_format_regex_for_effective_date(self):

        data = [['019', '68752267', '4518403', 5, '2020-08-06 00:00.']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    def test_2_date_format_regex_for_effective_date(self):

        data = [['019', '68752267', '4518403', 5, '2020-08-06 00:00.ddd']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        with self.assertRaises(ValueError):
            validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


if __name__ == '__main__':
    unittest.main(verbosity=2, warnings='ignore')
