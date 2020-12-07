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

    def test_get_opco_list(self):

        data = [['018', '118101', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118102', '4119062', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118106', '4119063', '5', '2020-08-06 00:00:00.000000'],
                ['019', '118106', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '196668', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['010', '687522', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.get_opco_list(df)
        self.assertEqual(len(result), 4, "It should contain 3 opcos")
        self.assertEqual(result, ['019', '020', '018', '010'], "It should contain 3 opcos")

    def test_filtration_of_records_of_failed_opcos(self):

        data = [['018', '118101', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118102', '4119062', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118106', '4119063', '5', '2020-08-06 00:00:00.000000'],
                ['019', '118106', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '196668', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['010', '687522', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        result = validator.remove_records_of_given_opcos(df, ['020', '018'])
        self.assertEqual(result.count(), 3, "It should contain the other records")
        self.assertEqual(result.filter("opco_id == '018'").count(), 0, "It should not contain records for opco 018")
        self.assertEqual(result.filter("opco_id == '020'").count(), 0, "It should not contain records for opco 020")

    def test_filtration_of_records_of_failed_opcos_none(self):

        data = [['018', '118101', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118102', '4119062', '5', '2020-08-06 00:00:00.000000'],
                ['020', '118106', '4119063', '5', '2020-08-06 00:00:00.000000'],
                ['019', '118106', '9002908', '1', '2020-08-06 00:00:00.000000'],
                ['019', '196668', '3555349', '1', '2020-08-06 00:00:00.000000'],
                ['010', '687522', '4518403', '5', '2020-08-06 00:00:00.000000']]

        schema = StructType([
            StructField("opco_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("supc", StringType(), True),
            StructField("price_zone", StringType(), True),
            StructField("effective_date", StringType(), True)]
        )
        df = self.spark.createDataFrame(data=data, schema=schema)

        self.assertEqual(validator.remove_records_of_given_opcos(df, []).count(), 6,
                         "It should contain all the records")


    def test_data_with_one_invalid_opco_id_and_valid_opco_id_list(self):

        data = [['018', '', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['020', '', '4119062', '5', '2020-08-06 00:00:00.000000'],
                ['020', '', '4119063', '5', '2020-08-06 00:00:00.000000'],
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

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(len(result), 2, "It should only return ['020', '018']")
        self.assertIn('018', result, "It should '018'")
        self.assertIn('020', result, "It should '020'")

    def test_data_without_invalid_opco_id(self):

        data = [['018', '112', '4119061', '5', '2020-08-06 00:00:00.000000'],
                ['020', '34', '4119062', '5', '2020-08-06 00:00:00.000000'],
                ['020', '123', '4119063', '5', '2020-08-06 00:00:00.000000'],
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

        result = validator.validate_column(df, 'customer_id')
        self.assertEqual(result, [], "It should not return anything")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


if __name__ == '__main__':
    unittest.main(verbosity=2, warnings='ignore')
