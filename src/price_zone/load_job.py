import sys
import threading
import time
import json

import boto3
import base64
import pymysql

import sqlalchemy as sqlalchemy
from queue import Queue
from sqlalchemy.pool import QueuePool
from awsglue.utils import getResolvedOptions

lambda_client = boto3.client('lambda')

query_for_tables = 'SELECT * FROM settings WHERE setting = (%s)'

charset = 'utf8'
cursor_type = pymysql.cursors.DictCursor

def _execute_load(pool, queue, database, table, threadErrors):
    while not queue.empty():
        s3_file_path = queue.get()
        try:
            table_with_database_name = database + Configuration.DOT + table
            load_timestamp = str(int(time.time()))
            load_qry = "LOAD DATA FROM S3 '" + s3_file_path + "'" \
                                                              "REPLACE INTO TABLE " + table_with_database_name + \
                       " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' " \
                       "IGNORE 1 LINES (@supc,@price_zone,@customer_id,@effective_date) SET " \
                       "SUPC=@supc," \
                       "CUSTOMER_ID=@customer_id," \
                       "EFFECTIVE_DATE=@effective_date," \
                       "PRICE_ZONE=@price_zone," \
                       "ARRIVED_TIME=" + data_arrival_timestamp + "," \
                       "UPDATED_TIME=" + load_timestamp + ";"

            connection = pool.connect()
            print("Populating price zone data from file: %s to table %s with load time %s\n" % (s3_file_path,
                                                                                                table_with_database_name,
                                                                                                load_timestamp))
            connection.execute(load_qry)
            print(
                "Completed loading price zone data from s3 %s to table %s\n" % (s3_file_path, table_with_database_name))
            connection.close()
        except Exception as e:
            print("Error occurred when trying to file: %s, error: %s" % (s3_file_path, repr(e)))
            threadErrors.append(repr(e))
            raise e


def load_data(dbconfigs, opco_id, bucketname, partitioned_files_path):
    prefix = partitioned_files_path + Configuration.OUTPUT_PATH_PREFIX + opco_id
    output_files = list_files_in_s3(bucketname, prefix)
    dbconfigs['database'] = Configuration.DATABASE_PREFIX + opco_id
    pool = __create_db_engine(dbconfigs)
    queue = Queue()
    for file in output_files:
        prefix_path = prefix + '/'
        if file['Key'] != prefix_path:
            s3_file_path = "s3://" + bucketname + "/" + file['Key']
            queue.put(s3_file_path)

    threadErrors = []
    threads = []
    for i in range(0, 4):
        threads.append(threading.Thread(target=_execute_load,
                                        args=(pool, queue, dbconfigs['database'], dbconfigs['table'], threadErrors)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    if len(threadErrors) > 0:
        print(threadErrors)
        raise Exception(threadErrors)


def getNewConnection(host, user, decrypted ,db):
    return pymysql.connect(host=host, user=user, password=decrypted, db=db)


def _retrieve_conection_details():
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_connection(Name=glue_connection_name)

    connection_properties = response['Connection']['ConnectionProperties']
    URL = connection_properties['JDBC_CONNECTION_URL']
    url_list = URL.split("/")

    host = "{}".format(url_list[-2][:-5])
    port = url_list[-2][-4:]
    user = "{}".format(connection_properties['USERNAME'])
    pwd = "{}".format(connection_properties['ENCRYPTED_PASSWORD'])

    session = boto3.session.Session()
    client = session.client('kms', region_name='us-east-1')
    decrypted = client.decrypt(
        CiphertextBlob=bytes(base64.b64decode(pwd))
    )
    return {
        'username': user,
        'password': decrypted['Plaintext'].decode("utf-8"),
        'host': host,
        'port': int(port),
        "decrypted": decrypted
    }

def get_values_from_ssm(keys):
    client_ssm = boto3.client('ssm')
    response = client_ssm.get_parameters(Names=keys)
    parameters = response['Parameters']
    invalidParameters = response['InvalidParameters']

    if invalidParameters:
        raise KeyError('Found invalid ssm parameter keys:' + ','.join(invalidParameters))

    parameter_dictionary = {}
    for parameter in parameters:
        parameter_dictionary[parameter['Name']] = parameter['Value']

    return parameter_dictionary

def get_connection_details(env):
    db_url = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/DB_URL'
    password = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/PASSWORD'
    username = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/USERNAME'
    db_name = '/CP/' + env + '/ETL/REF_PRICE/PRICE_ZONE/DB_NAME'
    print(db_url, password, username, db_name)
    ssm_keys = [db_url, password, username, db_name]
    ssm_key_values = get_values_from_ssm(ssm_keys)
    print(ssm_key_values)
    return {
        "db_url": ssm_key_values[db_url],
        "password": ssm_key_values[password],
        "username": ssm_key_values[username],
        "db_name": ssm_key_values[db_name]
    }

def get_db_connection(env):
    connection_params = get_connection_details(env)
    return pymysql.connect(
        host=connection_params['db_url'], user=connection_params['username'], password=connection_params['password'], db=connection_params['db_name'], charset=charset, cursorclass=cursor_type)

def get_active_and_future_tables(env ,table):
    #from common db
    database_connection = get_db_connection(env)
    try:
        cursor_object = database_connection.cursor()

        sql = "SELECT Value FROM settings WHERE setting = '%s'"%table
        print(sql)
        cursor_object.execute(sql)
        result = cursor_object.fetchall()
        print(result)
        print(result[0]['Value'])
        return result[0]['Value']
    except Exception as e:
        print(e)
    finally:
        database_connection.close()

def check_table_is_empty(table,db_configs):
    print('check if tables are empty')
    # here not common db connection ,connection to ref dbs has to be taken here
    #use the passed db config since it contains whch table
    database_connection = getNewConnection(db_configs['host'], db_configs['username'], db_configs['password'], db_configs['database'])

    try:
        cursor_object = database_connection.cursor()
        sql = "SELECT * FROM " + table + " LIMIT 1"
        print(sql)
        cursor_object.execute(sql)
        result = cursor_object.fetchall()
        print(result)
        return result
    except Exception as e:
        print(e)
    finally:
        database_connection.close()

def find_tables_to_load(partial_load ,env ,opco_id, intermediate_s3, partitioned_files_key):
    db_configs = _retrieve_conection_details()

    if partial_load:
        active_table = get_active_and_future_tables(env, "ACTIVE_TABLE")
        future_table = get_active_and_future_tables(env, "FUTURE_TABLE")
        print(active_table, future_table)
        #find active table from db

        #load data to active table
        db_configs['table'] = active_table
        load_data(db_configs, opco_id, intermediate_s3, partitioned_files_key)

        #check whether future table is empty
        db_configs['table'] = future_table
        future_table_query_result = check_table_is_empty(future_table, db_configs)
        print(future_table_query_result)

        if len(future_table_query_result) == 0:
            #future table empty stop
            print('future table is empty , therefore stop the process')
        else:
            #load future table
            db_configs['table'] = future_table
            print('future table is not empty, therefore load the future table')
            load_data(db_configs, opco_id, intermediate_s3, partitioned_files_key)

    else:
        future_table = get_active_and_future_tables(env, "FUTURE_TABLE")
        future_table_query_result = check_table_is_empty(future_table, db_configs)
        if len(future_table_query_result) == 0:
            print('future table empty, therefore load to future table ')
            #load future table
            db_configs['table'] = future_table
            load_data(db_configs, opco_id, intermediate_s3, partitioned_files_key)
        else:
            print('future table is non empty')
            raise Exception("Sorry, future table is not empty")


def list_files_in_s3(bucketname, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucketname, Prefix=prefix)
    matchingObjects = []
    for page in pages:
        matchingObjects.extend(page['Contents'])

    return matchingObjects


class Configuration:
    DOT = "."

    TABLE_NAME = "PRICE_ZONE_01"
    OUTPUT_PATH_PREFIX = "/opco_id="  # opco_id substring depends on the column naming at spark job
    DATABASE_PREFIX = "REF_PRICE_"


def __create_db_engine(credentials):
    connect_url = sqlalchemy.engine.url.URL(
        'mysql+pymysql',
        username=credentials['username'],
        password=credentials['password'],
        host=credentials['host'],
        port=credentials['port'],
        database=credentials['database'])
    return sqlalchemy.create_engine(connect_url, poolclass=QueuePool, pool_size=5, max_overflow=10, pool_timeout=30)



if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['opco_id', 'partitioned_files_key', 'etl_timestamp', 'partial_load', 'intermediate_s3_name', 'intermediate_directory_path', 'GLUE_CONNECTION_NAME', 'METADATA_LAMBDA'])
    glue_connection_name = args['GLUE_CONNECTION_NAME']
    opco_id = args['opco_id']  # opco_id validation
    partitioned_files_key = args['partitioned_files_key']
    intermediate_s3 = args['intermediate_s3_name']
    data_arrival_timestamp = args['etl_timestamp']
    metadata_lambda = args['METADATA_LAMBDA']
    intermediate_directory_path = args['intermediate_directory_path']
    partial_load = args['partial_load']

    print(
        "Started data loading job for Opco: %s, file path: %s/%s\n" % (opco_id, intermediate_s3, partitioned_files_key))

    find_tables_to_load(bool(partial_load), "DEV", opco_id, intermediate_s3, partitioned_files_key)
