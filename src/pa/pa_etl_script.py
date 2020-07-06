import sys

import boto3
import pandas as pd
import base64
import datetime

import pymysql
from awsglue.utils import getResolvedOptions
from urllib.parse import urlparse


def read_data_from_s3(bucketname, key):
    print("Starting downloading Price advisor data file from S3")
    s3 = boto3.client('s3')
    s3.download_file(bucketname, key, Configuration.FILE_NAME)
    print("Completed downloading Price advisor data file from S3")
    return pd.read_csv(Configuration.FILE_NAME, ",")


def write_dataframe_to_s3(opco_id, fileName):
    print("starting uploading Price Advisor data of opco %s to s3 file %s" % (opco_id, fileName))
    key = Configuration.OUTPUT_FILE_PATH + fileName
    with open(fileName, 'rb') as data:
        s3_output = boto3.client('s3')
        s3_output.upload_fileobj(data, Configuration.BUCKET_NAME, key)

    print("Completed uploading Price Advisor data of opco %s to s3 file %s" % (opco_id, fileName))


def load_data(opco_id, df):
    output_file_name = Configuration.OUTPUT_FILE_PREFIX + "_" + opco_id + Configuration.CSV
    del df['opco_id']
    df.to_csv(output_file_name, encoding='utf-8', index=False)
    write_dataframe_to_s3(opco_id, output_file_name)

    connectionDetails = getConnectionDetails()
    conn = getNewConnection(connectionDetails["host"], connectionDetails["user"],
                            connectionDetails["decrypted"])

    table_with_database_name = Configuration.DATABASE_PREFIX + opco_id + Configuration.DOT + Configuration.TABLE_NAME
    s3_output_file_path = "s3://" + Configuration.BUCKET_NAME + "/" + Configuration.OUTPUT_FILE_PATH + output_file_name
    cur = conn.cursor()

    createTableIfnotExistsQuery = "CREATE TABLE IF NOT EXISTS " + table_with_database_name + "(" \
                                                                                             "`SUPC` VARCHAR(9) NOT NULL," \
                                                                                             "  `PRICE_ZONE` TINYINT(1) UNSIGNED NOT NULL," \
                                                                                             "  `PRICE` DOUBLE(13 , 3) NOT NULL," \
                                                                                             "  `EFFECTIVE_DATE` DATETIME NOT NULL," \
                                                                                             "  `EXPORTED_DATE` BIGINT(10) UNSIGNED NOT NULL," \
                                                                                             "  PRIMARY KEY (`SUPC`,`PRICE_ZONE`,`EFFECTIVE_DATE`, `EXPORTED_DATE`)" \
                                                                                             ")  ENGINE=INNODB DEFAULT CHARSET=UTF8;"

    loadQry = "LOAD DATA FROM S3 '" + s3_output_file_path + "' " \
                                                            "REPLACE INTO TABLE " + table_with_database_name + \
              " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
              "IGNORE 1 LINES (@item_id, @price_zone_id, @new_price, @new_price_effective_date, " \
              "@export_date, @isPound) SET " \
              "SUPC=@item_id," \
              "PRICE_ZONE=@price_zone_id," \
              "PRICE=@new_price," \
              "EXPORTED_DATE=@export_date," \
              "EFFECTIVE_DATE=@new_price_effective_date," \
              "ISPOUND=@isPound;"

    cur.execute(createTableIfnotExistsQuery)
    print("Loading PA data to the table : %s from file %s\n" % (table_with_database_name, s3_output_file_path))
    cur.execute(loadQry)
    conn.commit()
    print("Successfully populated PA data to the table : %s from file %s\n" % (
    table_with_database_name, s3_output_file_path))
    conn.close()


def getConnectionDetails():
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(Name=Configuration.GLUE_CONNECTION_NAME)

    connection_properties = response['Connection']['ConnectionProperties']
    URL = connection_properties['JDBC_CONNECTION_URL']
    url_list = URL.split("/")

    host = "{}".format(url_list[-2][:-5])
    user = "{}".format(connection_properties['USERNAME'])
    pwd = "{}".format(connection_properties['ENCRYPTED_PASSWORD'])

    session = boto3.session.Session()
    client = session.client('kms', region_name='us-east-1')
    decrypted = client.decrypt(
        CiphertextBlob=bytes(base64.b64decode(pwd))
    )

    return {"host": host,
            "user": user,
            "decrypted": decrypted}


class Configuration:
    UNDERSCORE = "_"
    DOT = "."
    CSV = ".csv"

    TABLE_NAME = "PA"
    DATABASE_PREFIX = "REF_PRICE_"
    GLUE_CONNECTION_NAME = 'rp-data-populator-poc-connection'
    BUCKET_NAME = "cp-ref-price-poc-bucket"
    OUTPUT_FILE_PREFIX = "pa_data_output"
    FILE_NAME = "pa_data.csv.gz"
    OUTPUT_FILE_PATH = "output/"


def getNewConnection(host, user, decrypted):
    return pymysql.connect(host=host, user=user, password=decrypted["Plaintext"])


def validate_data(dataframe):
    if dataframe.isnull().values.any():
        raise Exception('dataframe should not contain null values')


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_path'])
    inputFilePath = args['s3_path']
    print("Started ETL process for Price Advisor data in file %s\n" % inputFilePath)

    parsed_path = urlparse(inputFilePath, allow_fragments=False)
    df = read_data_from_s3(bucketname=parsed_path.netloc, key=parsed_path.path.lstrip('/'))
    df.columns = ["item_id",
                  "price_zone_id",
                  "new_price",
                  "new_price_effective_date",
                  "current_price",
                  "reason_code",
                  "export_date"]

    df["new_price"] = df["new_price"].apply(lambda x: x.strip('$'))
    df["new_price_effective_date"] = df["new_price_effective_date"].apply(
        lambda x: datetime.datetime.strptime(x, "%m/%d/%Y"))
    df['export_date'] = df["export_date"].apply(lambda x: datetime.datetime.strptime(x, "%m/%d/%Y").timestamp())
    del df['current_price']
    del df['reason_code']

    df["opco_id"] = df["price_zone_id"].apply(lambda x: x.split('-')[0])
    df["price_zone_id"] = df["price_zone_id"].apply(lambda x: x.split('-')[1])

    item_zone_prices_for_opco = dict(tuple(df.groupby(df['opco_id'])))  # group data by opco_id

    for opco in item_zone_prices_for_opco:
        # opco_id validation
        load_data(opco, item_zone_prices_for_opco[opco])
