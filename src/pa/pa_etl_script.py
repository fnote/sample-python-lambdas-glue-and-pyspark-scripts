import sys

import boto3
import pandas as pd
import base64
from datetime import datetime

import pymysql
from awsglue.utils import getResolvedOptions
from urllib.parse import urlparse


def read_data_from_s3(bucketname, key):
    print("Starting downloading Price advisor data file from S3")
    s3 = boto3.client('s3')
    s3.download_file(bucketname, key, Configuration.FILE_NAME)
    print("Completed downloading Price advisor data file from S3")
    return pd.read_csv(Configuration.FILE_NAME, sep="|")


def write_dataframe_to_s3(opco_id, fileName):
    print("starting uploading Price Advisor data of opco %s to s3 file %s" % (opco_id, fileName))
    intermediate_storage_parsed_path = urlparse(intermediate_storage_path, allow_fragments=False)
    intermediate_bucket = intermediate_storage_parsed_path.netloc
    key = intermediate_storage_parsed_path.path.lstrip('/') + Configuration.OUTPUT_FILE_PATH + fileName
    with open(fileName, 'rb') as data:
        s3_output = boto3.client('s3')
        s3_output.upload_fileobj(data, intermediate_bucket, key)

    print("Completed uploading Price Advisor data of opco %s to s3 bucket: %s key:%s" % (opco_id, intermediate_bucket, key))


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

    loadQry = "LOAD DATA FROM S3 '" + s3_output_file_path + "' " \
                                                            "REPLACE INTO TABLE " + table_with_database_name + \
              " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
              "IGNORE 1 LINES (@supc, @price_zone_id, @new_price, @new_price_effective_date, " \
              "@export_date, @split_indicator) SET " \
              "SUPC=@supc," \
              "PRICE_ZONE=@price_zone_id," \
              "PRICE=@new_price," \
              "EXPORTED_DATE=@export_date," \
              "EFFECTIVE_DATE=@new_price_effective_date," \
              "SPLIT_INDICATOR=@split_indicator;"

    print("Loading PA data to the table : %s from file %s\n" % (table_with_database_name, s3_output_file_path))
    cur.execute(loadQry)
    conn.commit()
    print("Successfully populated PA data to the table : %s from file %s\n" % (
    table_with_database_name, s3_output_file_path))
    conn.close()


def getConnectionDetails():
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(Name=glue_connection_name)

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
    BUCKET_NAME = "cp-ref-price-poc-bucket"
    OUTPUT_FILE_PREFIX = "pa_data_output"
    FILE_NAME = "pa_data.csv.gz"
    OUTPUT_FILE_PATH = "output/"


def getNewConnection(host, user, decrypted):
    return pymysql.connect(host=host, user=user, password=decrypted["Plaintext"])

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_path', 'intermediate_storage_path', 'glue-connection-name'])
    inputFilePath = args['s3_path']
    intermediate_storage_path = args['intermediate_storage_path']
    glue_connection_name = args['glue-connection-name']

    print("Started ETL process for Price Advisor data in file %s\n" % inputFilePath)

    parsed_path = urlparse(inputFilePath, allow_fragments=False)
    df = read_data_from_s3(bucketname=parsed_path.netloc, key=parsed_path.path.lstrip('/'))

    del df['CURRENT_PRICE']
    del df['REASON']
    del df['LOCAL_REFERENCE_PRICE']

    df = df.rename(columns={'ITEM_ID': 'supc'})
    df = df.rename(columns={'NEW_PRICE': 'new_price'})
    df = df.rename(columns={'ITEM_ATTR_5_NM': 'split_indicator'})

    df["effective_date"] = df["EFFECTIVE_DATE"].apply(
        lambda x: datetime.strptime(x.split()[0], "%Y-%m-%d"))
    df['export_date'] = df["EXPORT_DATE"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp())
    df["opco_id"] = df["PRICE_ZONE_ID"].apply(lambda x: x.split('-')[0])
    df["price_zone_id"] = df["PRICE_ZONE_ID"].apply(lambda x: x.split('-')[1])

    item_zone_prices_for_opco = dict(tuple(df.groupby(df['opco_id'])))  # group data by opco_id

    # opco_id validation
    for opco in item_zone_prices_for_opco:
        load_data(opco, item_zone_prices_for_opco[opco])
