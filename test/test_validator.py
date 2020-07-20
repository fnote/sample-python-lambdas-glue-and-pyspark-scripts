import pytest
from pyspark.sql import SparkSession
from src.price_zone import validator

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master("local").appName("testing").getOrCreate()

def test_null_data(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EAT_sameple.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')
