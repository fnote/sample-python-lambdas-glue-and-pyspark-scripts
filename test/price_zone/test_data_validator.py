import unittest
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_timestamp

from src.price_zone import validator
from src.price_zone.constants import SUPC_LENGTH, CUST_NBR_LENGTH, PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE, \
    DATE_FORMAT_REGEX, INPUT_DATE_FORMAT, OUTPUT_DATE_FORMAT, CO_NBR_LENGTH


class TestSparkDataframeValidator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)
    # todo change this
    def test_with_valid_data(self):
        data = [['019', '104612', '1234567', '5', '2020-08-06 00:00:00.000000']]
        active_opcos = ['019', '020']
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)
        try:
            invalid_opcos = []
            validator.validate_opcos(df, active_opcos, 'opco_id')

            invalid_opcos.extend(validator.validate_column(df, 'customer_id'))
            invalid_opcos.extend(validator.validate_column(df, 'supc'))
            invalid_opcos.extend(validator.validate_column(df, 'price_zone'))
            invalid_opcos.extend(validator.validate_date_format(df, 'eff_from_dttm', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT))

            invalid_opcos.extend(validator.validate_column_length_less_than(df, 'customer_id', CUST_NBR_LENGTH))
            invalid_opcos.extend(validator.validate_column_length_less_than(df, 'supc', SUPC_LENGTH))

            df = df.withColumn("price_zone", df["price_zone"].cast(IntegerType()))
            invalid_opcos.extend(validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE))

            df = df.withColumn('effective_date', to_timestamp(df['eff_from_dttm'], OUTPUT_DATE_FORMAT))
            invalid_opcos.extend(validator.validate_date_time_field(df, 'effective_date'))
            df.show(truncate=False)
            self.assertEqual(invalid_opcos, [], "It should return invalid OpCo ids")

        except ValueError:
            self.fail("Should fail. Received ValueError for valid data")

    def test_inactive_opcos_in_file(self):
        """PRCP-2109"""

        data = [['019', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        active_opcos = ['021', '020']

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'supc')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'supc')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'supc')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column_length_less_than(df, 'supc', SUPC_LENGTH)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_non_numeric_data_for_customer_id(self):
        """PRCP-2011"""

        data = [['019', '57936$', '1272772', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column_length_less_than(df, 'customer_id', CUST_NBR_LENGTH)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

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

        result = validator.validate_column(df, 'price_zone')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_empty_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['019', '579369', '1272772', '', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'price_zone')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_non_numeric_data_for_price_zone(self):
        """PRCP-2013"""

        data = [['019', '579369', '1272772', '&', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'price_zone')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_range_of_price_zone_with_value_less_than_min(self):
        """PRCP-2013"""

        data = [['019', '579369', '1272772', 0, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_range_of_price_zone_with_value_greater_than_max(self):
        """PRCP-2013"""

        data = [['019', '579369', '1272772', 6, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_customer_id_and_valid_customer_id_list(self):
        """PRCP-2016"""

        data = [['019', '', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['019', '810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', '5', '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_supc_and_valid_supc_list(self):
        """PRCP-2017"""

        data = [['019', '810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '480111', '1#$%^&', '5', '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'supc')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_price_zone_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['019', '480111', '4119061', None, '2020-08-06 00:00:00.000000'],
                ['019', '810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_column(df, 'price_zone')
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_price_zone_out_of_range_and_valid_price_zone_list(self):
        """PRCP-2018"""

        data = [['019', '480111', '4119061', 1, '2020-08-06 00:00:00.000000'],
                ['019', '810622', '9002908', 11, '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', 5, '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)])

        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_data_range(df, 'price_zone', PRICE_ZONE_MIN_VALUE, PRICE_ZONE_MAX_VALUE)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_containing_one_empty_row(self):
        """PRCP-2065"""

        data = [['019', '480111', '4119061', '2', '2020-08-06 00:00:00.000000'],
                ['019', '810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', '5', '2020-08-06 00:00:00.000000'],
                ['', '', '', '', '']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        self.assertEqual(validator.validate_column(df, 'customer_id'), [''], "It should return invalid OpCo ids")
        self.assertEqual(validator.validate_column(df, 'supc'), [''], "It should return invalid OpCo ids")
        self.assertEqual(validator.validate_column(df, 'price_zone'), [''], "It should return invalid OpCo ids")

    def test_data_containing_one_null_row(self):
        """PRCP-2065"""

        data = [['019', '480111', '4119061', '2', '2020-08-06 00:00:00.000000'],
                ['019', '810622', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['019', '752266', '4518403', '5', '2020-08-06 00:00:00.000000'],
                [None, None, None, None, None]]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        self.assertEqual(validator.validate_column(df, 'customer_id'), [None], "It should return invalid OpCo ids")
        self.assertEqual(validator.validate_column(df, 'supc'), [None], "It should return invalid OpCo ids")
        self.assertEqual(validator.validate_column(df, 'price_zone'), [None], "It should return invalid OpCo ids")

    def test_null_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '752267', '4518403', 5, None]]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_empty_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '752267', '4518403', 5, '']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_non_numeric_data_for_effective_date(self):
        """PRCP-2015"""

        data = [['019', '752267', '4518403', 5, 'abc']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_1_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/MM/yyyy"""

        data = [['019', '752267', '4518403', 5, '25/10/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_2_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format dd/yyyy/MM"""

        data = [['019', '752267', '4518403', 5, '25/2020/10']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_3_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format MM/yyyy/dd"""

        data = [['019', '752267', '4518403', 5, '12/2020/26']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_4_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/MM/dd"""

        data = [['019', '752267', '4518403', 5, '2020/08/28']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_5_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format yyyyy/dd/MM"""

        data = [['019', '752267', '4518403', 5, '2020/22/07']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_6_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format Month/dd/yyyy"""

        data = [['019', '752267', '4518403', 5, 'June/10/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_7_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format 01 and 1"""

        data = [['019', '752267', '4518403', 5, '01/15/2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_8_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format year as yy"""

        data = [['019', '752267', '4518403', 5, '01/15/20']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_9_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format No split"""

        data = [['019', '752267', '4518403', 5, '1 15 2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_10_invalid_format_data_for_effective_date(self):
        """PRCP-2015 invalid format wrong split"""

        data = [['019', '752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_effective_date_and_valid_effective_date_list(self):
        """PRCP-2020"""

        data = [['019', '810622', '9002908', 1, '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['018', '480111', '4518408', 5, '2020-08-06 00:00:00.000000'],
                ['018', '752267', '4518403', 5, '1-15-2020']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['018'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_effective_date_value_and_valid_effective_date_list(self):

        data = [['019', '810622', '9002908', 1, '2020-08-06 00:00:00.000000'],
                ['019', '666867', '3555349', 1, '2020-08-06 00:00:00.000000'],
                ['019', '480111', '4518408', 5, '2020-08-06 00:00:00.000000'],
                ['018', '752267', '4518403', 5, '2/30/2019']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        df = df.withColumn('effective_date', to_timestamp(df['eff_from_dttm'], OUTPUT_DATE_FORMAT))
        result = validator.validate_date_time_field(df, 'effective_date')
        self.assertEqual(result, ['018'], "It should return invalid OpCo ids")

    def test_validate_date_time_field(self):

        data = [['019', '810622', '9002908', 1, '2020-08-06 00:00:00']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("eff_from_dttm", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        df = df.withColumn('effective_date', to_timestamp(df['eff_from_dttm'], OUTPUT_DATE_FORMAT))
        df.show(truncate=False)
        results = validator.validate_date_time_field(df, 'effective_date')
        self.assertEqual(results, [], "It should return invalid OpCo ids")

    def test_null_data_for_opco_id(self):

        active_opcos = ['019', '020']
        data = [[None, '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, [None], "It should return invalid OpCo ids")

    def test_empty_data_for_opco_id(self):

        active_opcos = ['019', '020']
        data = [['', '104612', '', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, [''], "It should return invalid OpCo ids")

    def test_non_numeric_data_for_opco_id(self):

        active_opcos = ['019', '020']
        data = [['abc', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, ['abc'], "It should return invalid OpCo ids")

    def test_data_length_for_opco_id_for_when_input_length_is_high(self):

        active_opcos = ['019', '020']
        data = [['0190', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, ['0190'], "It should return invalid OpCo ids")

    def test_data_length_for_opco_id_for_when_input_length_is_less(self):

        active_opcos = ['019', '020']
        data = [['19', '104612', '1234567', 5, '2020-08-06 00:00:00.000000']]
        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, ['19'], "It should return invalid OpCo ids")

    def test_data_with_one_invalid_opco_id_and_valid_opco_id_list(self):

        active_opcos = ['019', '020']
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

        result = validator.validate_opcos(df, active_opcos, 'opco_id')
        self.assertEqual(result, ['0109', ''], "It should return invalid OpCo ids")

    def test_1_date_format_regex_for_effective_date(self):

        data = [['019', '752267', '4518403', 5, '2020-08-06 00:00.']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    def test_2_date_format_regex_for_effective_date(self):

        data = [['019', '752267', '4518403', 5, '2020-08-06 00:00.ddd']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", IntegerType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.validate_date_format(df, 'effective_date', DATE_FORMAT_REGEX, INPUT_DATE_FORMAT)
        self.assertEqual(result, ['019'], "It should return invalid OpCo ids")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


if __name__ == '__main__':
    unittest.main(verbosity=2, warnings='ignore')
