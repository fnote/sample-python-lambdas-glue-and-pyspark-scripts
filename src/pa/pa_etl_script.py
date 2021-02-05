import sys
import time

import boto3
import json
import pandas as pd
import numpy as np
import base64
from datetime import datetime

import pymysql
from awsglue.utils import getResolvedOptions

lambda_client = boto3.client('lambda')


def read_data_from_s3(bucketname, key):
    print("Starting downloading Price advisor data file from S3")
    s3 = boto3.client('s3')
    s3.download_file(bucketname, key, Configuration.FILE_NAME)
    print("Completed downloading Price advisor data file from S3")
    return pd.read_csv(Configuration.FILE_NAME, sep="|", dtype={'ITEM_ID': np.str, 'LOCAL_REFERENCE_PRICE': np.str})


def write_dataframe_to_s3(opco_id, fileName):
    print("starting uploading PA data of opco %s to s3 file %s" % (opco_id, fileName))
    key = output_file_path + fileName
    with open(fileName, 'rb') as data:
        s3_output = boto3.client('s3')
        s3_output.upload_fileobj(data, intermediate_s3_bucket, key)

    print("Completed uploading PA data of opco %s to s3 bucket: %s key:%s" % (
    opco_id, intermediate_s3_bucket, key))


def load_data(opco_id, df):
    output_file_name = Configuration.OUTPUT_FILE_PREFIX + "_" + opco_id + Configuration.CSV
    del df['opco_id']
    df.to_csv(output_file_name, encoding='utf-8', index=False)
    write_dataframe_to_s3(opco_id, output_file_name)

    connectionDetails = getConnectionDetails()
    conn = getNewConnection(connectionDetails["host"], connectionDetails["user"],
                            connectionDetails["decrypted"])

    table_with_database_name = Configuration.DATABASE_PREFIX + opco_id + Configuration.DOT + Configuration.TABLE_NAME
    s3_output_file_path = "s3://" + intermediate_s3_bucket + "/" + output_file_path + output_file_name
    cur = conn.cursor()

    load_timestamp = str(int(time.time()))

    loadQry = "LOAD DATA FROM S3 '" + s3_output_file_path + "' " \
                                                            "REPLACE INTO TABLE " + table_with_database_name + \
              " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
              "IGNORE 1 LINES (@supc,@effective_date,@price,@export_date,@catch_weight_indicator," \
              "@price_zone_id) SET " \
              "SUPC=@supc," \
              "PRICE_ZONE=@price_zone_id," \
              "PRICE=@price," \
              "EXPORTED_DATE=@export_date," \
              "EFFECTIVE_DATE=@effective_date," \
              "CATCH_WEIGHT_INDICATOR=@catch_weight_indicator," \
              "ARRIVED_TIME=" + data_arrival_timestamp + "," \
              "UPDATED_TIME=" + load_timestamp + ";"

    print("Loading PA data to the table : %s from file %s with load timestamp %s\n" % (table_with_database_name,
                                                                                       s3_output_file_path,
                                                                                       load_timestamp))
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

    TABLE_NAME = "PRICE"
    DATABASE_PREFIX = "REF_PRICE_"
    OUTPUT_FILE_PREFIX = "pa_data_output"
    FILE_NAME = "pa_data.csv.gz"


def getNewConnection(host, user, decrypted):
    return pymysql.connect(host=host, user=user, password=decrypted["Plaintext"])


def validate_price(df, column):
    df[column] = pd.to_numeric(df[column])
    invalid_df = df[df[column] <= 0].dropna()
    if len(invalid_df.head(1)) > 0:
        print(invalid_df)
        print("price cannot be negative or zero : ", column)
        return len(invalid_df)
    else:
        return 0


def write_metadata(metadata_lambda, intermediate_s3_name, intermediate_directory_path, count_from_file, invalid_price_record_count):
    response = lambda_client.invoke(FunctionName=metadata_lambda, Payload=json.dumps({
        "intermediate_s3_name": intermediate_s3_name,
        "intermediate_directory_path": intermediate_directory_path,
        "total_record_count_from_pa_file": count_from_file,
        "invalid_price_record_count": invalid_price_record_count,
    }))

    return response


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_input_bucket', 's3_input_file_key', 'etl_timestamp', 'etl_output_path_key',
                                         'INTERMEDIATE_S3_BUCKET', 'GLUE_CONNECTION_NAME', 'METADATA_LAMBDA', 'intermediate_directory_path'])
    s3_input_bucket = args['s3_input_bucket']
    s3_input_file_key = args['s3_input_file_key']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    glue_connection_name = args['GLUE_CONNECTION_NAME']
    data_arrival_timestamp = args['etl_timestamp']
    etl_timestamp = args['etl_timestamp']
    metadata_lambda = args['METADATA_LAMBDA']
    intermediate_directory_path = args['intermediate_directory_path']

    output_file_path = args['etl_output_path_key'] + "/"

    print("Started ETL process for PA data in bucket %s with key %s\n" % (s3_input_bucket, s3_input_file_key))

    df = read_data_from_s3(bucketname=s3_input_bucket, key=s3_input_file_key)

    del df['CURRENT_PRICE']
    del df['REASON']
    del df['NEW_PRICE']

    invalid_price_record_count = validate_price(df, 'LOCAL_REFERENCE_PRICE')

    df = df.rename(columns={'ITEM_ID': 'supc'})
    df = df.rename(columns={'LOCAL_REFERENCE_PRICE': 'price'})
    df = df.rename(columns={'ITEM_ATTR_5_NM': 'catch_weight_indicator'})

    df["EFFECTIVE_DATE"] = df["EFFECTIVE_DATE"].apply(
        lambda x: datetime.strptime(x.split()[0], "%Y-%m-%d"))
    df['EXPORT_DATE'] = df["EXPORT_DATE"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp())
    df["opco_id"] = df["PRICE_ZONE_ID"].apply(lambda x: x.split('-')[0])
    df["price_zone_id"] = df["PRICE_ZONE_ID"].apply(lambda x: x.split('-')[1])

    df = df.rename(columns={'EFFECTIVE_DATE': 'effective_date'})
    df = df.rename(columns={'EXPORT_DATE': 'export_date'})
    del df['PRICE_ZONE_ID']

    # finally setting column order
    df = df[['supc', 'effective_date', 'price', 'export_date', 'catch_weight_indicator', 'price_zone_id', 'opco_id']]

    total_record_count_from_pa_file = len(df.index)

    item_zone_prices_for_opco = dict(tuple(df.groupby(df['opco_id'])))  # group data by opco_id

    for opco in item_zone_prices_for_opco:
        load_data(opco, item_zone_prices_for_opco[opco])

    write_metadata(metadata_lambda, intermediate_s3_bucket, intermediate_directory_path, total_record_count_from_pa_file ,invalid_price_record_count)