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

OPCO_CLUSTER_MAPPINGS_QUERY = 'SELECT * FROM OPCO_CLUSTER WHERE OPCO_ID IN ({})'
JOB_EXECUTION_STATUS_UPDATE_QUERY = 'UPDATE LOAD_JOB_EXECUTION_STATUS SET TOTAL_ACTIVE_OPCO_COUNT = {},FAILED_OPCO_IDS = "{}", TOTAL_RECORD_COUNT = "{}",INVALID_RECORD_COUNT = "{}",RECEIVED_OPCOS = "{}" WHERE FILE_NAME="{}" AND ETL_TIMESTAMP={}'
glue_connection_name = 'cp-ref-etl-common-connection-{}-cluster-{}'
CLUSTER_ID_COLUMN_NAME = 'CLUSTER_ID'
OPCO_ID_COLUMN_NAME = 'OPCO_ID'
charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys, WithDecryption=True)
    print(response)
    parameters = response['Parameters']
    invalid_parameters = response['InvalidParameters']
    if invalid_parameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalid_parameters))
    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']
    print(parameter_dictionary)
    return parameter_dictionary


def get_common_db_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/COMMON/DB_NAME'
    ssm_keys = [db_url, db_name, username, password]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_endpoint": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }


def create_common_db_connection(env):
    connection_params = get_common_db_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_endpoint'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)


def get_opco_cluster_mapping(opcos, env):
    database_connection = create_common_db_connection(env)
    try:
        cursor_object = database_connection.cursor()
        query = OPCO_CLUSTER_MAPPINGS_QUERY.format(opcos)
        print(query)
        cursor_object.execute(query)
        result = cursor_object.fetchall()
        print(result)
        return result
    except Exception as e:
        print(e)
    finally:
        database_connection.close()


def separate_opcos_by_cluster(mappings, active_opco_list):
    cluster_1_opcos = []
    cluster_2_opcos = []
    invalid_or_inactive_opcos = []
    for mapping in mappings:
        cluster_id = mapping[CLUSTER_ID_COLUMN_NAME]
        opco_id = mapping[OPCO_ID_COLUMN_NAME]
        if cluster_id == '01' and opco_id in active_opco_list:
            cluster_1_opcos.append(opco_id)
        elif cluster_id == '02' and opco_id in active_opco_list:
            cluster_2_opcos.append(opco_id)
        else:
            invalid_or_inactive_opcos.append(opco_id)

    result_list = [cluster_1_opcos, cluster_2_opcos, invalid_or_inactive_opcos]
    print(result_list)
    return result_list


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

    print("Completed uploading PA data of opco %s to s3 bucket: %s key:%s" % (opco_id, intermediate_s3_bucket, key))


def load_data(opco_id, df ,cluster_id):
    output_file_name = Configuration.OUTPUT_FILE_PREFIX + "_" + opco_id + Configuration.CSV
    del df['opco_id']
    df.to_csv(output_file_name, encoding='utf-8', index=False)
    write_dataframe_to_s3(opco_id, output_file_name)

    connectionDetails = getConnectionDetails(cluster_id)

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


def getConnectionDetails(cluster_id):
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(Name=glue_connection_name.format(environment, cluster_id))

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
        "received_records_count": count_from_file,
        "invalid_price_record_count": invalid_price_record_count,
    }))

    return response


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['s3_input_bucket', 's3_input_file_key', 'etl_timestamp', 'etl_output_path_key',
                                         'INTERMEDIATE_S3_BUCKET', 'METADATA_LAMBDA', 'ENV', 'intermediate_directory_path'])
    s3_input_bucket = args['s3_input_bucket']
    s3_input_file_key = args['s3_input_file_key']
    intermediate_s3_bucket = args['INTERMEDIATE_S3_BUCKET']
    data_arrival_timestamp = args['etl_timestamp']
    etl_timestamp = args['etl_timestamp']
    metadata_lambda = args['METADATA_LAMBDA']
    intermediate_directory_path = args['intermediate_directory_path']
    environment = args['ENV']

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

    #get unique opco ids from file
    unique_opco_ids = list(set(df['opco_id'].tolist()))
    joined_string = ",".join(unique_opco_ids)

    #get the cluster details of those opcos
    try:
        opco_cluster_mapping = get_opco_cluster_mapping(joined_string, environment)

        # add active opco list here
        cluster1_opcos_cluster2_opcos_and_invalids = separate_opcos_by_cluster(opco_cluster_mapping, unique_opco_ids)
        df_cluster_1 = df[df['opco_id'].isin(cluster1_opcos_cluster2_opcos_and_invalids[0])]
        df_cluster_2 = df[df['opco_id'].isin(cluster1_opcos_cluster2_opcos_and_invalids[1])]
        failed_opco_list_string = joined_string = ",".join(cluster1_opcos_cluster2_opcos_and_invalids[2])
        total_opcos = len(cluster1_opcos_cluster2_opcos_and_invalids[0]) + len(cluster1_opcos_cluster2_opcos_and_invalids[1])

        total_record_count_from_pa_file = len(df.index)

        # load price data to cluster 1
        item_zone_prices_for_opco_in_cluster_1 = dict(tuple(df_cluster_1.groupby(df_cluster_1['opco_id'])))  # group data by opco_id
        for opco in item_zone_prices_for_opco_in_cluster_1:
            print('load data in to cluster : 1')
            load_data(opco, item_zone_prices_for_opco_in_cluster_1[opco], '01')

        # load price data to cluster 2
        item_zone_prices_for_opco_in_cluster_2 = dict(tuple(df_cluster_2.groupby(df_cluster_2['opco_id'])))  # group data by opco_id
        for opco in item_zone_prices_for_opco_in_cluster_2:
            print('load data in to cluster : 2')
            load_data(opco, item_zone_prices_for_opco_in_cluster_2[opco], '02')

        write_metadata(metadata_lambda, intermediate_s3_bucket, intermediate_directory_path, total_record_count_from_pa_file, invalid_price_record_count)

        # write to db
        database_connection = create_common_db_connection(environment)
        cursor_object = database_connection.cursor()
        # add failed opcos here and increment failed opcos count
        cursor_object.execute(JOB_EXECUTION_STATUS_UPDATE_QUERY.format(total_opcos, failed_opco_list_string, str(total_record_count_from_pa_file),
                                                                       str(invalid_price_record_count), joined_string, s3_input_file_key,
                                                                       etl_timestamp))
        database_connection.commit()

    except Exception as e:
        raise e
