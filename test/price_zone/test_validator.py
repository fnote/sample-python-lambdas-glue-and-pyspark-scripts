import pytest
from src.price_zone import validator

def test_null_data(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EATs_Test_01.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')

def test_empty_data(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EATs_Test_01.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')

def test_nan_data(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EATs_Test_01.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')

def test_data_length(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EATs_Test_01.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')

def test_data_range(spark):
    df = spark.read.format("csv").option("header", "true").load('./data/EATs_Test_01.csv')
    with pytest.raises(ValueError):
        validator.validate_column(df, 'supc')